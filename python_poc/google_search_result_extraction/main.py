import argparse
from datetime import datetime
import pandas as pd
from loguru import logger
from datetime import date
import uuid

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium_extraction_methods import determine_result_type, extract_published_date_from_result_elements, SearchResultType

parser = argparse.ArgumentParser(
    prog="google_search_result_ingestor"
)

parser.add_argument("query")
parser.add_argument("-e", "--env_file")
parser.add_argument("-p", "--pages", type=int)
parser.add_argument("-n", "--node_id")
parser.add_argument("-a", "--after_date")
parser.add_argument("-b", "--before_date")
parser.add_argument("-i", "--interval")

args = parser.parse_args()

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
datelist: list[datetime] = pd.date_range(start=args.after_date, end=before_date, periods=num_days.days).to_pydatetime().tolist()

logger.info(f"Attempting to extract all search results for query {full_query}")

count = 0
for day in datelist[0:2]:

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
        next_page_link =  table_component.find_element(By.ID, "pnnext")

    elif len(next_a_tag) == 0:
        logger.debug("Only a single page found for query. Extracting all search results for single page")
        next_page_link = None
    else:
        raise ValueError(f"Error in trying to find the <a> tag for the next google search page. Number of <a> tags found: {len(next_a_tag)}")

    num_pages = 0
    while (next_page_link != None and num_pages <= total_num_pages):

        center_col_all_content_lst = driver.find_elements(By.ID, "search")
        if len(center_col_all_content_lst) != 1:
            raise ValueError(f"Center element id='search' not found. {len(center_col_all_content_lst)}")
        center_col_all_content = center_col_all_content_lst[0]

        container_for_results = center_col_all_content.find_element(By.ID, "rso")
        all_result_elements = container_for_results.find_elements(By.XPATH, "./child::*") 
        logger.debug(f"{len(all_result_elements)} elements found")

        for result in all_result_elements:

            driver.implicitly_wait(1)

            try:
                result_type: SearchResultType = determine_result_type(result)
                url = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                title = result.find_element(By.TAG_NAME, "h3").text
                published_date_text = extract_published_date_from_result_elements(result, result_type=result_type)
            except:
                logger.warning(f"Unable to find the Url or Title for result component. Skipping processing this result component")
                continue

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
    
        elif len(next_a_tag) == 0:
            logger.debug("Only a single page found for query. Extracting all search results for single page")
            next_page_link = None
            logger.debug("Next page link set to None. Reached the end of the search query. Returning driver")
        else:
            raise ValueError(f"Multiple next <a> tags found in the Google search table components {len(next_a_tag)}")
        num_pages += 1