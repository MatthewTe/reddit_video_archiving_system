from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from data_writers.custom_types.search_results_types import SearchResultType


from loguru import logger

def determine_result_type(reslt_webelement_component: WebElement) -> SearchResultType:

    url = reslt_webelement_component.find_element(By.TAG_NAME, "a").get_attribute("href")

    if ".pdf" in url:
        logger.info(f"Assigned url: {url} as type PDF")
        return SearchResultType.PDF_LINK
    elif "www.youtube.com" in url:
        logger.info(f"Assigned url: {url} as type Youtube Video")
        return SearchResultType.YOUTUBE_VIDEO
    else:
        logger.info(f"Not able to assign a direct type for url: {url}. Setting result type to basic link")
        return SearchResultType.BASIC_LINK
    

def extract_published_date_from_result_elements(result_webelement_component: WebElement, result_type: SearchResultType) -> str:
    if result_type == SearchResultType.BASIC_LINK or result_type == SearchResultType.PDF_LINK:
        logger.info(f"Result type is a basic link or pdf like. Running published date extraction logic.")
        published_date = result_webelement_component.find_element(By.XPATH, "//*[contains(@class, 'LEwnzc') and contains(@class, 'Sqrs4e')]")
        published_date_text = published_date.find_element(By.TAG_NAME, "span").text

        return published_date_text

    elif result_type == SearchResultType.YOUTUBE_VIDEO:
        logger.info(f"Result type is a youtube. Attempting to open a new tab to load the youtube video to get additional information.")
        return "youtube"

def extract_additional_info_clickable_element(result_webelement_component: WebElement, result_type: SearchResultType) ->  WebElement:

    if result_type == SearchResultType.BASIC_LINK or result_type == SearchResultType.PDF_LINK:
        logger.info(f"Result type is a basic link or pdf like. Running clickable Web Element extraction for this result type")
        additional_info_btn = result_webelement_component.find_element(By.CSS_SELECTOR, '[aria-label="About this result"]')
        return additional_info_btn
    else:
        raise AttributeError(f"Result type not supported - not extracting an additional info button from web element. Current result type for element: {result_type}")
 