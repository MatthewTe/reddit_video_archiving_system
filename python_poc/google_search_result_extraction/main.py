import argparse
from datetime import datetime
import pandas as pd
from loguru import logger
from datetime import date
import uuid
import random
import sys
import time
import requests

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium_extraction_methods import determine_result_type, extract_published_date_from_result_elements, SearchResultType

from config import get_secrets, Secrets

parser = argparse.ArgumentParser(
    prog="google_search_result_ingestor"
)

parser.add_argument("query")
parser.add_argument("-e", "--env_file")
parser.add_argument("-p", "--pages", type=int)
parser.add_argument("-n", "--node_id")
parser.add_argument("-a", "--after_date")
parser.add_argument("-b", "--before_date")
#parser.add_argument("-i", "--interval")

args = parser.parse_args()

secrets: Secrets = get_secrets(args.env_file)

logger.info(f"Connecting results to node: {args.node_id}")

full_query = args.query
if args.pages is None:
    total_num_pages = 0
else:
    total_num_pages = args.pages

if args.after_date is not None:
    logger.info(f"After Date {args.after_date}")
    after_date: datetime = datetime.strptime(args.after_date, "%Y-%m-%d").date()
    #full_query += f" :after:{args.after_date}"

if args.before_date is not None:
    logger.info(f"Before Date {args.before_date}")
    before_date: datetime = datetime.strptime(args.before_date, "%Y-%m-%d").date()
    #full_query += f" :before:{args.before_date}"

num_days = before_date - after_date
datelist: list[datetime] = pd.date_range(start=after_date, end=before_date, periods=num_days.days).to_pydatetime().tolist()

logger.info(f"Attempting to extract all search results for query {full_query}")

count = 0
for day in datelist:

    count += 1

    if count == len(datelist):
        continue

    next_period = datelist[count]

    day_str = day.strftime("%Y-%m-%d")
    next_date_str = next_period.strftime("%Y-%m-%d")

    daily_query = f"{full_query} after:{day_str} before:{next_date_str} -site:youtube"

    driver = webdriver.Chrome()
    
    driver.get("https://www.google.com/")

    google_search_box = driver.find_element(By.TAG_NAME, "textarea")

    google_search_box.send_keys(daily_query)
    google_search_box.send_keys(Keys.RETURN)

    driver.implicitly_wait(2)

    next_a_tag = driver.find_elements(By.TAG_NAME, 'table')
    if len(next_a_tag) == 1:
        logger.debug(f"Multiple pages found for query. Iterating through all google pages")
        table_component = next_a_tag[0]
        try:
            next_page_link =  table_component.find_element(By.ID, "pnnext")
        except Exception as e:
            next_page_link = None

    elif len(next_a_tag) == 0:
        logger.debug("Only a single page found for query. Extracting all search results for single page")
        next_page_link = None
    else:
        raise ValueError(f"Error in trying to find the <a> tag for the next google search page. Number of <a> tags found: {len(next_a_tag)}")

    if next_page_link is None:
        logger.warning("No next page found - performing ingesting and extraction for only a single page")
        center_col_all_content_lst = driver.find_elements(By.ID, "search")
        if len(center_col_all_content_lst) != 1:
            raise ValueError(f"Center element id='search' not found. {len(center_col_all_content_lst)}")
        center_col_all_content = center_col_all_content_lst[0]

        try:
            container_for_results = center_col_all_content.find_element(By.ID, "rso")
            all_result_elements = container_for_results.find_elements(By.XPATH, "./child::*") 
            logger.debug(f"{len(all_result_elements)} elements found")
        except Exception as e:
            logger.warning(e)
            all_result_elements = []

        nodes_edges_to_add_upload = []
        for result in all_result_elements:

            driver.implicitly_wait(1)

            try:
                result_type: SearchResultType = determine_result_type(result)
                url = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                title = result.find_element(By.TAG_NAME, "h3").text
                published_date = extract_published_date_from_result_elements(result, result_type=result_type)
            except Exception as e:
                logger.warning(f"""Unable to find the Url or Title for result component. Skipping processing this result component
                        - error: {str(e)}
                """)
                continue

            search_result_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
            entity_day_str = published_date.strftime("%Y-%m-%d")

            # Data ingestion:
            serach_result_node = {
                "type": "node",
                "query_type": "MERGE",
                "labels":["Google", "Search", "Event"],
                "properties": {
                    "id": search_result_id,
                    "title": title,
                    "url": url,
                    "date":entity_day_str
                }
            }
            nodes_edges_to_add_upload.append(serach_result_node )
            logger.info(f"Added node for {title} search result to list")

            date_node = {
                "type":"node",
                "query_type": "MERGE",
                "labels":['Date'],
                "properties": {
                    "id": str(uuid.uuid3(uuid.NAMESPACE_URL, entity_day_str)),
                    "day":entity_day_str
                }
            }
            nodes_edges_to_add_upload.append(date_node)
            logger.info(f"Added date node for {entity_day_str} to list")

            search_result_to_edge_day = {
                "type":"edge",
                "labels": ["POSTED_ON"],
                "connection": {
                    "from": search_result_id,
                    "to":str(uuid.uuid3(uuid.NAMESPACE_URL, entity_day_str))
                },
                "properties": {}
            }
            nodes_edges_to_add_upload.append(search_result_to_edge_day)
            logger.info(f"Added edge between node({search_result_id})->date({entity_day_str}) to list")

            search_result_parent_node_edge = {
                "type":"edge",
                "labels":['ASSOCIATED_WITH'],
                "connection": {
                    "from": search_result_id,
                    "to": args.node_id
                },
                "properties": {}
            }
            nodes_edges_to_add_upload.append(search_result_parent_node_edge)
            logger.warning(f"Adding edge between parent_node({args.node_id})->node({search_result_id}) to list")

            try:
                node_ingestion_response = requests.post(f"{secrets['neo4j_url']}/v1/api/run_query", json=nodes_edges_to_add_upload)
                node_ingestion_response.raise_for_status()
                created_search_results = node_ingestion_response.json()
            except requests.HTTPError as e:
                logger.error(f"""Unable to add nodes and edges for current page. Request returned with error: {str(e)} \n
                    - {node_ingestion_response.content} \n
                    - Request content: {nodes_edges_to_add_upload}
                """)
                sys.exit(1)
            
            logger.info(f"Sucessfully inserted {len(nodes_edges_to_add_upload)} records into the graph database")
        else:

            num_pages = 0
            while (next_page_link != None and num_pages <= total_num_pages):

                center_col_all_content_lst = driver.find_elements(By.ID, "search")
                if len(center_col_all_content_lst) != 1:
                    raise ValueError(f"Center element id='search' not found. {len(center_col_all_content_lst)}")
                center_col_all_content = center_col_all_content_lst[0]

                container_for_results = center_col_all_content.find_element(By.ID, "rso")
                all_result_elements = container_for_results.find_elements(By.XPATH, "./child::*") 
                logger.debug(f"{len(all_result_elements)} elements found")

                nodes_edges_to_add_upload = []
                for result in all_result_elements:

                    driver.implicitly_wait(1)

                    try:
                        result_type: SearchResultType = determine_result_type(result)
                        url = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                        title = result.find_element(By.TAG_NAME, "h3").text
                        published_date = extract_published_date_from_result_elements(result, result_type=result_type)
                    except Exception as e:
                        logger.warning(f"""Unable to find the Url or Title for result component. Skipping processing this result component
                                - error: {str(e)}
                        """)
                        continue

                    search_result_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
                    entity_day_str = published_date.strftime("%Y-%m-%d")

                    # Data ingestion:
                    serach_result_node = {
                        "type": "node",
                        "query_type": "MERGE",
                        "labels":["Google", "Search", "Event"],
                        "properties": {
                            "id": search_result_id,
                            "title": title,
                            "url": url,
                            "date":entity_day_str
                        }
                    }
                    nodes_edges_to_add_upload.append(serach_result_node )
                    logger.info(f"Added node for {title} search result to list")

                    date_node = {
                        "type":"node",
                        "query_type": "MERGE",
                        "labels":['Date'],
                        "properties": {
                            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, entity_day_str)),
                            "day":entity_day_str
                        }
                    }
                    nodes_edges_to_add_upload.append(date_node)
                    logger.info(f"Added date node for {entity_day_str} to list")

                    search_result_to_edge_day = {
                        "type":"edge",
                        "labels": ["POSTED_ON"],
                        "connection": {
                            "from": search_result_id,
                            "to":str(uuid.uuid3(uuid.NAMESPACE_URL, entity_day_str))
                        },
                        "properties": {}
                    }
                    nodes_edges_to_add_upload.append(search_result_to_edge_day)
                    logger.info(f"Added edge between node({search_result_id})->date({entity_day_str}) to list")

                    search_result_parent_node_edge = {
                        "type":"edge",
                        "labels":['ASSOCIATED_WITH'],
                        "connection": {
                            "from": search_result_id,
                            "to": args.node_id
                        },
                        "properties": {}
                    }
                    nodes_edges_to_add_upload.append(search_result_parent_node_edge)
                    logger.warning(f"Adding edge between parent_node({args.node_id})->node({search_result_id}) to list")

                    try:
                        node_ingestion_response = requests.post(f"{secrets['neo4j_url']}/v1/api/run_query", json=nodes_edges_to_add_upload)
                        node_ingestion_response.raise_for_status()
                        created_search_results = node_ingestion_response.json()
                    except requests.HTTPError as e:
                        logger.error(f"""Unable to add nodes and edges for current page. Request returned with error: {str(e)} \n
                            - {node_ingestion_response.content} \n
                            - Request content: {nodes_edges_to_add_upload}
                        """)
                        sys.exit(1)
                    
                    logger.info(f"Sucessfully inserted {len(nodes_edges_to_add_upload)} records into the graph database")

                next_page_link.click()

                next_a_tag = driver.find_elements(By.TAG_NAME, 'table')
                if len(next_a_tag) == 1:

                    logger.debug(f"Multiple pages found for query. Iterating through all google pages")

                    table_component = next_a_tag[0]
                    
                    try:
                        next_page_link_components = table_component.find_elements(By.ID, "pnnext")
                        if len(next_page_link_components) == 1:
                            next_page_link =  next_page_link_components[0]
                        elif len(next_page_link_components) == 0:
                            next_page_link = None
                            logger.debug("Next page link set to None. Reached the end of the search query. Returning driver")
                    except Exception as e:
                        logger.error(f"Unable to extract next url from webpage: {str(e)}")
                        next_page_link = None
                        
                elif len(next_a_tag) == 0:
                    logger.debug("Only a single page found for query. Extracting all search results for single page")
                    next_page_link = None
                    logger.debug("Next page link set to None. Reached the end of the search query. Returning driver")
                else:
                    raise ValueError(f"Multiple next <a> tags found in the Google search table components {len(next_a_tag)}")
                num_pages += 1
                time.sleep(random.randint(1, 4))