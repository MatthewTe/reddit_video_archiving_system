from data_writers.html_parsers.html_parsers_bs4 import extract_author_from_result_component_html, extract_published_date_from_result_component_html
from data_writers.custom_types.search_results_types import SearchResultType

from data_readers.read_search_results import get_search_results

path_to_basic_link_html_component = "/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/python_poc/example_google_events_selenium/testing_data/search_result_html_components/0a83cfbc-6be1-368d-b3f9-db67bdd35b3e_BASIC_LINK_OUTER_HTML.html"
path_to_youtube_link_html_component = "/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/python_poc/example_google_events_selenium/testing_data/search_result_html_components/fb22dac6-18ab-3a55-8124-fe999d88e182_YOUTUBE_VIDEO_OUTER_HTML.html"
path_to_pdf_link_html_component = "/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/python_poc/example_google_events_selenium/testing_data/search_result_html_components/e31b5262-5de7-36d9-b1ae-2a8d6efc532d_PDF_LINK_OUTER_HTML.html"

if __name__ == '__main__':

    df = get_search_results('"North Korea"')
    df['extracted_date'] = df['result_html'].apply(lambda x: extract_published_date_from_result_component_html(result_html_component=x, result_type=SearchResultType.BASIC_LINK))
    print(df[['extracted_date', 'result_html']])