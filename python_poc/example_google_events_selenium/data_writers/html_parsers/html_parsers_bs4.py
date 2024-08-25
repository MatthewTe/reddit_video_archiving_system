import sys
import re
from bs4 import BeautifulSoup
from loguru import logger
from datetime import datetime

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
def extract_published_date_from_result_component_html(result_html_component: str, result_type: SearchResultType, **kwargs) -> datetime | None:

    soup = BeautifulSoup(result_html_component)

    match result_type:
        case SearchResultType.BASIC_LINK:
            
            def extract_date_string_from_span(tag):
                return tag.name == "span" and tag.has_attr("class") and "LEwnzc" in tag.get('class')

            inital_date_result: list = soup.find_all(extract_date_string_from_span)

            if len(inital_date_result) == 0:
                logger.warning(f"First pass at extracting the inital date result by css selector did not return a result")
                return None

            inital_date_str = inital_date_result[0].text
            extracted_datetime = None

            # Easy option - It just gives the date
            try:
                # Searching for a datetime pattern using regex :( :
                pattern = r'\b[A-Za-z]{3} \d{1,2}, \d{4}\b'
                match = re.search(pattern, inital_date_str)

                if not match:
                    raise ValueError(f"No match found for datetime string: {inital_date_str}. Skipping attempting datetime string match.")

                extracted_datetime = datetime.strptime(match.group(0), "%b %d, %Y")    
                logger.info(f"{inital_date_str} converted to datetime object {extracted_datetime}")
                return extracted_datetime
            except Exception as e:
                logger.warning(f"Unable to convert initial date string to datetime object with format %b %d, %Y - {inital_date_str}, \n Error: {str(e.with_traceback(None))}")

            # If text contains a relative date eg "4 days ago". This requires an extracted date to be passed into the function:
            if "ago" in inital_date_str:
                logger.info(f"ago substring found in iniital date text: {inital_date_str}. Starting to parse date using relative date logic")
                extracted_date: datetime | None = kwargs.get("extracted_date", None)
                if extracted_date is None:
                    logger.warning(f"Relative date logic requires kwarg 'extracted_date' but was not provided. Unable to parse relative date: {inital_date_str}")
                    return None
                else:
                    logger.warning(f"Relative date extraction currently not supported. Skipping parsing for {inital_date_str}")
                    return None

            return extracted_datetime
        
        case SearchResultType.PDF_LINK:
            pass

        case SearchResultType.YOUTUBE_VIDEO:
            pass


def extract_author_from_result_component_html(result_html_component: str, result_type: SearchResultType) -> str:
    
    soup = BeautifulSoup(result_html_component)
    
    print(soup)

    match result_type:
        case SearchResultType.BASIC_LINK:
            pass
        
        case SearchResultType.PDF_LINK:
            pass

        case SearchResultType.YOUTUBE_VIDEO:
            pass

