from bs4 import BeautifulSoup
from typing import TypedDict
from datetime import datetime
import uuid
from loguru import logger

class ArticleContent(TypedDict):
    title: str
    content: str
    published_date: str

class InternalArticleContent(TypedDict):
    title: str
    url: str
    published_date: str

class ArticlesDisplayPage(TypedDict):
    articles: list[InternalArticleContent]
    next_page: str


def extract_article_content(html_str: str) -> ArticleContent:
    soup = BeautifulSoup(html_str, features='html.parser')

    article_container = soup.find('div', class_="article-description")
    nested_paragraph_content = article_container.find_all("p")
    
    data = {}   

    publish_date_content = soup.find('span', class_='auther-dte')
    
    try:
        data_publish = publish_date_content['data-publish']
        dt = datetime.strptime(data_publish, "%B %d, %Y %I:%M %p")
        dt_str = dt.strftime("%Y-%m-%d")
        data['published_date'] = dt_str
        logger.info("Extracted article published date")

    except Exception as e:
        logger.warning(e.with_traceback())
        data_publish = publish_date_content.text
        data['published_date'] = data_publish
        logger.warning("Could not find article publish date - setting it to the default text")

    title_component = soup.find("h1")
    for title_descendent in title_component.descendants:
        if isinstance(title_descendent, str):
            data['title'] = title_descendent
            logger.info("Extracted title")

    raw_text = ''
    for paragraph in nested_paragraph_content:
        for element in paragraph.descendants:
            if isinstance(element, str):
                raw_text += element.replace("\n", "")

    data['content'] = raw_text
    logger.info("Extacted content")

    return data

def extract_articles_display_page(html_str: str) -> ArticlesDisplayPage:
    soup = BeautifulSoup(html_str, features="html.parser")

    articles = soup.find_all("div", class_='category_news_blk')
    data = {}
    article_lst = []

    for article in articles:
        article_dict = {}

        article_title_div = article.find("div", class_="brk-news-title")

        article_link_container = article_title_div.find("a", href=True)
        article_href = article_link_container['href']

        article_title_str = article_link_container.text

        article_dt = article.find("p", class_="auther-dte")

        try:
            data_publish = article_dt['data-publish']
            dt = datetime.strptime(data_publish, "%B %d, %Y %I:%M %p")
            dt_str = dt.strftime("%Y-%m-%d")
            article_dict['published_date'] = dt_str
            logger.info(f"Extracted article published date {dt_str}")

        except Exception as e:
            logger.warning(e.with_traceback())
            data_publish = article_dt.text
            article_dict['published_date'] = data_publish
            
            logger.warning("Could not find article publish date - setting it to the default text")

        article_dict['title'] = article_title_str
        logger.info(f"Extracting article title {article_title_str}")

        article_dict['url'] = article_href
        logger.info(f"Extracting article url {article_href}")

        article_dict["id"] = str(uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name=article_href))
        logger.info(f"Extracting article id from url {article_href}")

        article_lst.append(article_dict)
        logger.info(f"Adding article {article_title_str} to article list")

    data['articles'] = article_lst
    logger.info(f"Extracting {len(article_lst)} articles for the page")

    # Link to next page:
    next_page_a_tag = soup.find("a", title='Go to next page', href=True)
    data['next_page'] = next_page_a_tag['href']
    logger.info(f"Extracted the next page {data['next_page']}")

    return data
