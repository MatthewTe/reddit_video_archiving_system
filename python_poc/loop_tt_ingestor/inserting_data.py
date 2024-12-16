from loguru import logger
import requests
from config import Secrets, Article

def insert_articles(articles: list[Article], secrets: Secrets):
  
    # Building html insertion scripts:
    article_body = []
    for article in articles:
        article_body.append(
            {
                "type": "node",
                "query_type": "MERGE",
                "labels": ["Entity", "Article", "LootTT", "Trinidad", "Tobago"],
                "properties": {
                    "id": article["id"],
                    "title": article["title"],
                    "url": article['url'],
                    "type": article['type'],
                    "content": article["content"],
                    "source": article["source"],
                    "extracted_date": article['extracted_date'],
                    "published_date": article["published_date"],
                    "extracted_from": article['extracted_from'],
                    "article_category": article["article_category"]
                }
            }
        )

    try:
        loop_tt_article_creation_response = requests.post(f"{secrets['neo4j_url']}/v1/api/run_query", json=article_body)
        loop_tt_article_creation_response.raise_for_status()
        created_loop_tt_articles = loop_tt_article_creation_response.json()
        logger.info(f"Inserted loop tt posts into the database {created_loop_tt_articles}")

        return created_loop_tt_articles

    except requests.HTTPError as e:
        logger.error(
        f"""Unable to create reddit post. Request returned with error: {str(e)} \n
            - {loop_tt_article_creation_response.content}  \n
            - request object: {articles}
        """)
    
    return None