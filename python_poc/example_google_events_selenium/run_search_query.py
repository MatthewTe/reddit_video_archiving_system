from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd
import sys
import uuid
import sqlalchemy as sa
from loguru import logger

def ingest_search_results(search_query: str, date_range: dict[str | None, str | None], driver: webdriver.Chrome, db_engine: sa.engine.Engine) -> webdriver.Chrome:

    # TODO: Add support for only before and after queries not just both:
    if date_range['after'] is None and date_range['before'] is None:
        full_query = f'"{search_query}"'
    else:
        full_query = f'"{search_query}" after:{date_range["after"]} before:{date_range["before"]}'

    search_query_id = str(uuid.uuid3(uuid.NAMESPACE_URL, search_query))
    full_query_id = str(uuid.uuid3(uuid.NAMESPACE_URL, full_query))
    with db_engine.connect() as conn, conn.begin():
        result = conn.execute(
            sa.text("INSERT INTO search_queries (id, query, full_query_id, start_date, end_date, runtime) VALUES (:id, :query, :full_query_id, :start_date, :end_date, :runtime)"),
            {
                "id": search_query_id, 
                "query": search_query, 
                "full_query_id": full_query_id,
                "start_date": str(date_range['after']),
                "end_date": str(date_range['before']),
                "runtime": datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")}
        )
        
        if result.rowcount > 0:
            logger.debug("Inserted record of search query into the database")
        else:
            raise ValueError(f"Rowcount for insert query into search_query table was 0. Did not insert record of query into db {search_query_id}")

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
                driver.implicitly_wait(1)

                url = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                title = result.find_element(By.TAG_NAME, "h3").text
                published_date = result.find_element(By.XPATH, "//*[contains(@class, 'LEwnzc') and contains(@class, 'Sqrs4e')]")
                published_date_text = published_date.find_element(By.TAG_NAME, "span").text

                additional_info_btn = result.find_element(By.CSS_SELECTOR, '[aria-label="About this result"]')
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
                
                with db_engine.connect() as conn, conn.begin():
                    result = conn.execute(sa.text(
                        """
                        INSERT INTO search_results (id, query_id, full_query_id, title, url, published_date, modal)
                        SELECT :id, :query_id, :full_query_id, :title, :url, :published_date, :modal
                        WHERE NOT EXISTS (
                            SELECT 1 FROM search_results WHERE query_id = :query_id AND id = :id
                        )
                        """),
                        {
                            "id": url_hash,
                            "query_id": search_query_id,
                            "full_query_id": full_query_id,
                            "title":title,
                            "url":url,
                            "published_date": published_date_text, 
                            "modal":str(additional_info_component),
                        }
                    )

                    if result.rowcount > 0:
                        logger.debug(f"Row {url_hash}-{title} was inserted into the database")
                    else:
                        logger.warning(f"Row {url_hash}-{title} was not inserted into the database")
                        
                    

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
        

if __name__ == '__main__':

    sqlite_engine = sa.create_engine("sqlite:///test.sqlite")
    with sqlite_engine.connect() as conn, conn.begin():
        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_queries (
                id TEXT,
                full_query_id TEXT,
                query TEXT,
                start_date TEXT,
                end_date TEXT,
                runtime TEXT
            )
            """
        ))

        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_results (
                id TEXT,
                query_id TEXT,
                full_query_id TEXT,
                title TEXT,
                url TEXT,
                published_date TEXT,
                modal TEXT
            )
            """
        ))

    query = "North Korea"
    start_date = "2020-06-01"
    end_date = "2020-06-03"
    full_query = f'"{query}" after:{start_date} before:{end_date}'

    driver = webdriver.Chrome()

    driver = ingest_search_results(query, {'after':start_date, 'before': end_date}, driver, sqlite_engine)