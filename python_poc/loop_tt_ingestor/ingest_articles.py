import requests
import argparse
import random
import time
import pprint
import pandas as pd
from loguru import logger
from typing import TypedDict
from loguru import logger
from datetime import datetime
from urllib.parse import parse_qs

from config import LoopPageConfig, Article, get_secrets
from html_parsing import extract_article_content, extract_articles_display_page
from inserting_data import insert_articles

# Step 1: Make a request to the url page to get all of the posts for that page.
# Step 2: Query the database to get all of the unique articles that aren't in the database. 
# Step 3: For each article in unique article ingest it into the database.
# Step 4: Extract the link to the next page and recusviely move to Step 1.
parser = argparse.ArgumentParser()
#parser.add_argument("config", help="The full path to the config json file used to run the pipeline")
parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")

def process_loop_page(config: LoopPageConfig):

    query_param_str = config['query_param_str']
    article_category =  config['article_category']
    db_category = config['db_category']

    headers_lst = [
        {"User-Agent":'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'},
        {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'},
        {"User-Agent":'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1'},
        {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393'}
    ]

    base_url = f'https://tt.loopnews.com/category/{article_category}'
    article_base_url = 'https://tt.loopnews.com'
    query_params = parse_qs(query_param_str.lstrip("?"))

    article_extracted_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("Making request to article thumbnail page")
    articles_response: requests.Response = requests.get(base_url, params=query_params, headers=random.choice(headers_lst))
    logger.info(f"Article thumbnail http response: {articles_response.status_code}")

    if not articles_response.ok:
        logger.error(f"Unable to grab article for {articles_response.url}")
        logger.error(articles_response.content)
        return

    page_content = extract_articles_display_page(articles_response.content)
    logger.info(f"Extracted {len(page_content['articles'])} page content from {articles_response.url}")


    # Then we extract all of the article content from each of the pages:
    articles_to_insert: list[Article] = []
    if len(page_content['articles']) < 1:
        logger.error(f"No Articles found for page {articles_response.url}")
    else:
        # Checking the articles against existing articles in the db:
        try:
            current_post_ids = ",".join([article['id'] for article in page_content['articles']])
            unique_post_response = requests.get(f"{config['secrets']['neo4j_url']}/v1/api/exists", params={'post_ids': current_post_ids})
            logger.info(f"Made unique post determination request with post ids: {current_post_ids}")
            unique_post_response.raise_for_status()
            unique_post_json: list[dict] = unique_post_response.json()
            logger.info(f"Response from unique query: \n")
            pprint.pprint(unique_post_json)

        except requests.HTTPError as exception:
            logger.exception(f"Error in checking existing reddit posts from API {exception}")
            return

        duplicate_ids: list[str] = [article["id"] for article in unique_post_json if article['exists'] == True]
        unique_articles: list[dict] = [article for article in page_content["articles"] if article['id'] not in duplicate_ids]
        logger.info(f"Found {len(unique_articles)} unique articles that are not in the neo4j database. This differs from the total num articles {len(page_content['articles'])}")


        if len(unique_articles) == 0:
            logger.error("No unique articles found. Reached end of dataset. Stopping....")
            return

        for article in unique_articles:

            logger.info("Looping through articles for ingestion")
            logger.info("Making individual article http request")
            single_page_response = requests.get(f"{article_base_url}{article['url']}", headers=random.choice(headers_lst))
            logger.info(f"Full article http response: {single_page_response.status_code}")
            if not single_page_response.ok:
                logger.error(f"Error in getting page. Exited with status code {single_page_response.status_code}")
                logger.error(single_page_response.content)
                continue
            
            single_artice_content = extract_article_content(single_page_response.content)

            # Comparing the date from the article thumbnail on the articl display page conten to the date value from the article:
            if article["published_date"] == single_artice_content["published_date"]:
                logger.info("Published date from display page is the same as date from single article content date")
                article_published_date = single_artice_content["published_date"]
            else:
                logger.warning(f"Comparing publish dates is different: article_thumbnail: {article['published_date']} vs {single_artice_content['published_date']}")
                logger.warning("Setting to null.")
                continue
            
            new_article: Article = {
                "id": article['id'],
                "title": single_artice_content['title'],
                "url": article["url"],
                "type": config["db_category"],
                "content": single_artice_content["content"],
                "source": "https://tt.loopnews.com",
                "extracted_date": article_extracted_date,
                "published_date": article_published_date,
                "extracted_from": f"{article_base_url}/{article['url']}",
                "article_category": article_category
            }
        
            articles_to_insert.append(new_article)
            logger.info(f"Added article to article list {new_article}. List has {len(articles_to_insert)}")
            time.sleep(random.choice(range(1, 5)))

        if len(articles_to_insert) < 1:
            logger.warning("No articles sucessfully extracted. List is empty. Performing no ingestion")
        else:
            logger.info(f"Attempting to execute an ingestion query for the following articles: {articles_to_insert}")

            articles_insertion_response = insert_articles(articles_to_insert, secrets=config['secrets'])           
            if articles_insertion_response is None:
                logger.error("Error in inserting articles")

            if not page_content['next_page']:
                logger.warning("Next page not found for the article thumbnail page")
                return 
            

            new_config: LoopPageConfig = {
                'article_category': article_category,
                'db_category': db_category,
                'query_param_str': page_content['next_page'],
                'secrets': config["secrets"]
            }

            logger.info(f"Recursively parsing next page with new config {config['secrets']}")
            process_loop_page(new_config)

if __name__ == "__main__":

    crime_config: LoopPageConfig = {
        "query_param_str": '?page=0',
        "article_category": "looptt-crime",
        'db_category': "crime",
        "secrets": get_secrets()
    }

    process_loop_page(crime_config)   

    pass