
from bs4 import BeautifulSoup
from loguru import logger

from data_writers.custom_types.search_results_types import SearchResultType

def determine_result_type(url: str) -> SearchResultType:

    if ".pdf" in url:
        logger.info(f"Assigned url: {url} as type PDF")
        return SearchResultType.PDF_LINK
    elif "www.youtube.com" in url:
        logger.info(f"Assigned url: {url} as type Youtube Video")
        return SearchResultType.YOUTUBE_VIDEO
    else:
        logger.info(f"Not able to assign a direct type for url: {url}. Setting result type to basic link")
        return SearchResultType.BASIC_LINK

#TODO: Write this:
def extract_published_date_from_result_component_html(result_html_component: str, result_type: SearchResultType) -> str:
    pass

def extract_author_from_result_component_html(result_html_component: str, result_type: SearchResultType) -> str:
    pass