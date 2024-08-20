import typing
from selenium import webdriver
import sqlalchemy as sa
import datetime
import uuid
import time
from loguru import logger
import pandas as pd
from bs4 import BeautifulSoup

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from data_writers.custom_types.search_results_types import SearchQuery, SearchQueryResult
from python_poc.example_google_events_selenium.data_writers.html_parsers.google_search_parsers_selenium import (
    extract_published_date_from_result_elements, determine_result_type, 
    extract_additional_info_clickable_element
)
from data_writers.custom_types.search_results_types import SearchResultType

def ingest_search_results(
        search_query: str, 
        driver: webdriver.Chrome, 
        db_engine: sa.engine.Engine,
        **kwargs
    ) -> webdriver.Chrome:

    after_date = kwargs.get('after', "")
    before_date = kwargs.get('before', "")
    site = kwargs.get("site", "")
    filetype = kwargs.get('filetype', "")

    full_query = f'{search_query}'

    if after_date != "":
        full_query += f" after:{after_date}"
    if before_date != "":
        full_query += f" before:{before_date}"
    if site != "":
        full_query += f" site:{site}"
    if filetype != "":
        full_query += f" filetype:{filetype}"

    # Extracting ingestion functions:
    insert_search_query_sql: typing.Callable | None = kwargs.get("insert_search_query_func", None)
    if insert_search_query_sql is None:
        raise ValueError("No function provided that inserts records of a Search Query into a SQL database")

    insert_search_query_result_sql: typing.Callable | None = kwargs.get("insert_search_result_func", None)
    if insert_search_query_result_sql is None:
        raise ValueError("No function provided that inserts records of search query results into a SQL database")

    search_query_id = str(uuid.uuid3(uuid.NAMESPACE_URL, search_query))

    # Full query includes the run date in the hash for preserving distinction between runs of different date:
    current_day: str = datetime.datetime.now().strftime("%Y/%m/%d")
    full_query_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f"{full_query}-{current_day}"))

    search_query_insertion_dict: SearchQuery = {
        "id": search_query_id, 
        "query": search_query, 
        "full_query":full_query,
        "full_query_id": full_query_id,
        "start_date": after_date,
        "end_date": before_date,
        "site": site,
        "filetype": filetype,
        "runtime": datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    }
    insert_search_query_sql(db_engine, search_query_insertion_dict)

    driver.get("https://www.google.com/")

    google_search_box = driver.find_element(By.TAG_NAME, "textarea")

    google_search_box.send_keys(full_query)
    google_search_box.send_keys(Keys.RETURN)

    driver.implicitly_wait(2)

    next_a_tag = driver.find_elements(By.TAG_NAME, 'table')
    if len(next_a_tag) == 1:
        logger.debug(f"Multiple pages found for query. Iterating through all google pages")
        table_component = next_a_tag[0]
        next_page_link =  table_component.find_element(By.ID, "pnnext")

    elif len(next_a_tag) == 0:
        logger.debug("Only a single page found for query. Extracting all search results for single page")
        next_page_link = None
    else:
        raise ValueError(f"Error in trying to find the <a> tag for the next google search page. Number of <a> tags found: {len(next_a_tag)}")

    all_search_results: list[dict] = []

    while (next_page_link != None):

        center_col_all_content_lst = driver.find_elements(By.ID, "search")
        if len(center_col_all_content_lst) != 1:
            raise ValueError(f"Center element id='search' not found. {len(center_col_all_content_lst)}")
        center_col_all_content = center_col_all_content_lst[0]

        container_for_results = center_col_all_content.find_element(By.ID, "rso")
        all_result_elements = container_for_results.find_elements(By.XPATH, "./child::*") 
        logger.debug(f"{len(all_result_elements)} elements found")

        search_results = []

        for result in all_result_elements:

            try: 

                # Parsing the result type:
                driver.implicitly_wait(1)

                result_type: SearchResultType = determine_result_type(result)

                url = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                title = result.find_element(By.TAG_NAME, "h3").text

                published_date_text = extract_published_date_from_result_elements(result, result_type=result_type)

                try:               
                    additional_info_btn = extract_additional_info_clickable_element(result, result_type=result_type)
                except ValueError as e:
                    logger.warning(f"Additional info button not extracted from result element {title}. Skipping extracting further components for this result set")
                    continue

                additional_info_btn.click()
                
                driver.implicitly_wait(2)

                full_page_html = driver.page_source

                # Getting side component:
                soup = BeautifulSoup(full_page_html)

                additional_info_component = soup.find("div", id="Sva75c")
                if additional_info_component is None:
                    logger.warning(f"additional_info_component is set to None. No modal found, setting to empty string.")
                    additional_info_component = ""                   

                driver.implicitly_wait(1)
                
                url_hash = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
                result_html = result.get_attribute("outerHTML")

                search_query_result_insertion_dict: SearchQueryResult = {
                    "id": url_hash,
                    "query_id": search_query_id,
                    "full_query_id": full_query_id,
                    "title":title,
                    "url":url,
                    "published_date": published_date_text, 
                    "result_html": result_html,
                    "modal":str(additional_info_component),
                }

                insert_search_query_result_sql(db_engine, search_query_result_insertion_dict)
                
                time.sleep(1)

            except Exception as e:
                try:
                    logger.error(f"Error in {title} {e.with_traceback(None)}")
                except Exception as e:
                    logger.error(f"Error in processing article completley: {str(e.with_traceback(None))}")
                time.sleep(1)
                continue

        all_search_results.append(pd.DataFrame.from_dict(search_results)) 

        next_page_link.click()

        next_a_tag = driver.find_elements(By.TAG_NAME, 'table')
        if len(next_a_tag) == 1:

            logger.debug(f"Multiple pages found for query. Iterating through all google pages")

            table_component = next_a_tag[0]

            next_page_link_components = table_component.find_elements(By.ID, "pnnext")
            if len(next_page_link_components) == 1:
                next_page_link =  next_page_link_components[0]
            elif len(next_page_link_components) == 0:
                next_page_link = None
                logger.debug("Next page link set to None. Reached the end of the search query. Returning driver")
                return driver
    
        elif len(next_a_tag) == 0:
            logger.debug("Only a single page found for query. Extracting all search results for single page")
            next_page_link = None
            logger.debug("Next page link set to None. Reached the end of the search query. Returning driver")
            return driver
        else:
            raise ValueError(f"Multiple next <a> tags found in the Google search table components {len(next_a_tag)}")
 