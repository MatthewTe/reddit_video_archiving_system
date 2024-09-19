from typing import TypedDict
import uuid
import pandas as pd
import json
import requests
import io
import sys
import pprint
import time
import base64
from loguru import logger
from minio import Minio
from minio.error import S3Error

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from config import Secrets

class RedditPostDict(TypedDict):
    id: str
    subreddit: str
    url: str
    title: str
    static_downloaded_flag: bool
    screenshot_path: str 
    json_path: str
    created_date: str
    static_root_url: str
    static_file_type: str
def get_post_dict_from_element(post_element) -> RedditPostDict:

    reddit_post_id = post_element.get_attribute("id")
    title = post_element.find_element(By.CSS_SELECTOR, f"#{reddit_post_id} > div:nth-child(5) > div:nth-child(1) > p:nth-child(1) > a:nth-child(1)").text
    id = str(uuid.uuid3(namespace=uuid.NAMESPACE_DNS, name=reddit_post_id))

    subreddit = post_element.get_attribute("data-subreddit")
    url = f"https://www.reddit.com{post_element.get_attribute('data-permalink')}"

    static_downloaded = False
    screenshot = f"{id}/screenshot.png"
    json = f"{id}/post.json"

    post_unix_timestamp = post_element.get_attribute("data-timestamp")
    created_date = pd.to_datetime(int(post_unix_timestamp), utc=True, unit="ms").strftime("%Y-%m-%dT%H:%M:%SZ")

    static_root_url = f"{id}/"
    static_file_type = post_element.get_attribute(f"data-kind")

    post_data_dict: RedditPostDict = {
        "id": id,
        "subreddit": subreddit,
        "url": url,
        "title": title,
        "static_downloaded_flag": static_downloaded,
        "screenshot_path": screenshot,
        "json_path": json,
        "created_date": created_date,
        "static_root_url": static_root_url,
        "static_file_type": static_file_type
    }

    return post_data_dict

class RedditAuthorDict(TypedDict):
    author_name: str
    author_full_name: str

def get_author_dict_from_element(post_element) -> RedditAuthorDict:

    author_name = post_element.get_attribute("data-author")
    author_full_name = post_element.get_attribute("data-author-fullname")

    reddit_user: RedditAuthorDict = {
        "author_name": author_name, 
        "author_full_name": author_full_name
    }

    return reddit_user

class RedditCommentDict(TypedDict):
    reddit_post_id: str
    comment_id: str
    comment_body: str
    associated_user: RedditAuthorDict
    posted_timestamp: str

class RedditCommentAttachmentDict(TypedDict):
    reddit_post: RedditPostDict
    attached_comments: list[RedditCommentDict]

def get_comments_from_json(post: RedditPostDict, json_bytes_stream: io.BytesIO) -> RedditCommentAttachmentDict:

    try:
        json_bytes_stream.seek(0)
        
        row_reddit_json = json.loads(json_bytes_stream.read())
        comment_content = row_reddit_json[1]['data']['children']

        post_comment_dicts: list[RedditCommentDict] = []
        for comment_json in comment_content:
            
            comment_json = comment_json['data']

            try:
                author = comment_json['author']
            except Exception as e:
                logger.warning(f"Unable to extract author from post json so setting author to none: \n {pprint.pprint(comment_json)}")
                author = "not_found"

            if author == '[deleted]':
                associated_user: RedditUserDict = {
                    "author_full_name": "deleted_user",
                    "author_name": author
                }
            elif author == 'not_found':
                associated_user: RedditUserDict = {
                    "author_full_name": "not_found",
                    "author_name": author
                }
            else:
                associated_user: RedditUserDict = {
                    "author_full_name": comment_json['author_fullname'],
                    "author_name": author
                }

            #TODO: Add some error catching here:
            try:
                reddit_comment_dict: RedditCommentDict = {
                    "reddit_post_id":post['id'],
                    "comment_id":comment_json["id"],
                    "comment_body":comment_json["body"],
                    "associated_user": associated_user,
                    "posted_timestamp": pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
                }

                post_comment_dicts.append(reddit_comment_dict)
            except Exception as e:
                logger.warning(str(e))
                continue

        attached_reddit_comments: RedditCommentAttachmentDict = {
            "reddit_post": post,
            "attached_comments": post_comment_dicts
        }

        return attached_reddit_comments

    except Exception as e:
        logger.error(f"Error in extracting RedditCommentAttachmentDict from post json \n -err: {str(e)} \n post-{pprint.pprint(post)}")
        return None

    
def get_post_json(driver, url) -> io.BytesIO | None:

    time.sleep(1.5)

    try:

        driver.get(f"{url}.json")

        json_element = driver.find_element(By.XPATH, "/html/body/pre")
        json_dict =  json_element.get_attribute("innerText")

        json_bytes_stream = io.BytesIO()
        json_bytes_stream.write(json_dict.encode())

        time.sleep(1.5)

        return json_bytes_stream

    except requests.HTTPError as err:
        logger.error(f"""
            Unable to get the json representation of the post {url} \n
            - error: {str(err)}
        """)
        return None

def take_post_screenshot(driver, url) -> bytes:

    try:
        driver.get(url)
        driver.implicitly_wait(4000)

        time.sleep(5)

        screenshot_base64 = driver.get_screenshot_as_base64()
        screenshot_bytes: bytes = base64.b64decode(screenshot_base64)

        screenshot_bytes_stream = io.BytesIO()
        screenshot_bytes_stream.write(screenshot_bytes)

        return screenshot_bytes_stream
    except Exception as e:
        logger.error(f"""Error in trying to take a screenshot of page {url}
        - error {str(e)}
        """)
        return None

def insert_static_file_to_blob(memory_buffer: io.BytesIO, bucket_name: str, full_filepath: str, content_type: str, minio_client: Minio):
    
    try:

        memory_buffer.seek(0)

        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=full_filepath,
            data=memory_buffer,
            length=memory_buffer.getbuffer().nbytes,
            content_type=content_type
        )
        return full_filepath

    except S3Error as exc:
        print("Error in inserting file to blob: ", exc)
        return None

class RedditUserDict(TypedDict):
    author_name: str
    author_full_name: str

class RedditPostCreationResult(TypedDict):
    parent_post: RedditPostDict
    attached_user: RedditUserDict

def insert_reddit_post(post: RedditPostDict, reddit_user: RedditUserDict, secrets: Secrets) -> RedditPostCreationResult | None:

    try:
        reddit_post_creation_response = requests.post(f"{secrets['neo4j_url']}/v1/reddit/create_post", json=post)
        reddit_post_creation_response.raise_for_status()
        created_post: RedditPostDict = reddit_post_creation_response.json()
        logger.info(f"Inserterd reddit post into the database {created_post}")
    except requests.HTTPError as e:
        logger.error(
        f"""Unable to create reddit post. Request returned with error: {str(e)} \n
            - {reddit_post_creation_response.content}  \n
            - request object: {post}
        """)
        return None

    try:
        attached_user_respone = requests.post(f"{secrets['neo4j_url']}/v1/reddit/attach_user_to_post", json={
            "parent_post": created_post,
            "attached_user": reddit_user
        })
        attached_user_respone.raise_for_status()
        attached_response: RedditPostCreationResult = attached_user_respone.json()
        logger.info(f"Attached user to created post {attached_response}")
        return attached_response
    except requests.HTTPError as e:
        logger.error(
            f"""Unable to attach reddit user to created post - {str(e)} \n
            - {attached_user_respone.content} \n
            - request object: {attached_response} 
        """)
        return None

def attach_reddit_post_comments(attached_comment_dict: RedditCommentAttachmentDict, secrets: Secrets) -> RedditCommentAttachmentDict | None:
    
    try:
        attached_comment_response = requests.post(f"{secrets['neo4j_url']}/v1/reddit/append_comments", json=attached_comment_dict)
        attached_comment_response.raise_for_status()
        attached_comments: RedditCommentAttachmentDict = attached_comment_response.json()
        return attached_comments
    except requests.HTTPError as e:
        logger.error(
            f"""Unable to create comments and attach them to reddit post - {str(e)} \n
            - Post: {attached_comment_dict['reddit_post']} \n
            - Comments {attached_comment_dict['attached_comments']}
        """)
        return None



def recursive_insert_raw_reddit_post(driver: webdriver.Chrome, page_url: str, MINIO_CLIENT, BUCKET_NAME: str, secrets: Secrets, login: bool=False):
    driver.get(page_url)

    driver.implicitly_wait(3000)

    # Loging in:
    if login:
        login_button = driver.find_element(By.XPATH, "//a[@class='login-required login-link']")
        login_button.click()

        time.sleep(2)

        login_input = driver.find_element(By.ID, "login-username")
        password_input = driver.find_element(By.ID, "login-password")

        login_input.send_keys(secrets['reddit_username'])
        password_input.send_keys(secrets['reddit_password'])

        input("Press any key to resume...")

    time.sleep(5)

    posts_site_table = driver.find_element(By.ID, "siteTable")

    next_button_results: list = driver.find_elements(By.XPATH, "//span[@class='next-button']")
    if len(next_button_results) == 0:
        next_button_url = None
    else:
        next_button_url = next_button_results[0].find_element(By.TAG_NAME, "a").get_attribute("href")

    logger.info(f"Next url for next page: {next_button_url}")

    all_posts_on_page = posts_site_table.find_elements(By.XPATH, "//div[@data-context='listing']")
    
    posts_to_ingest: list[RedditPostDict] = []

    author_associated_w_posts: dict[str, RedditPostDict] = {}
    for post_element in all_posts_on_page:
        post_dict: RedditPostDict = get_post_dict_from_element(post_element)
        pprint.pprint(post_dict)
        author_dict: RedditAuthorDict = get_author_dict_from_element(post_element)
        author_associated_w_posts[post_dict['id']] = author_dict

        posts_to_ingest.append(post_dict)

    logger.info(f"Found a total of {len(posts_to_ingest)} posts from page {page_url}")

    try:
        unique_post_response = requests.get(f"{secrets['neo4j_url']}/v1/reddit/check_posts_exists", params={'reddit_post_ids': ",".join([post['id']for post in posts_to_ingest])})
        unique_post_response.raise_for_status()
        unique_post_json: list[dict] = unique_post_response.json()
    except requests.HTTPError as exception:
        logger.exception(f"Error in checking existing reddit posts from API {exception}")
        return

    duplicate_ids: list[str] = [post["id"] for post in unique_post_json if post['post_exists'] == True]
    unique_posts: list[RedditPostDict] = [post for post in posts_to_ingest if post['id'] not in duplicate_ids]
    logger.info(f"Found {len(unique_posts)} unique posts that are not in the reddit database")

    # Actually inserting the post data into the database:
    if len(unique_posts) == 0:
        logger.info("No unique posts found from reddit page. Exiting...")
        return

    logger.info(f"Beginning to process unique posts")
    for post in unique_posts:

        logger.info(f"Trying to take screenshot for {post['url']}")
        screenshot_stream: io.BytesIO | None = take_post_screenshot(driver, post['url'])
        if screenshot_stream is None:
            logger.error(f"""Screenshot bytes stream returned as none with error. Not inserting post {post['id']} \n
            - post {pprint.pprint(post)}
            """)
            continue

        logger.info(f"Extracting json representation of post {post['url']}")
        json_stream: io.BytesIO | None = get_post_json(driver, post["url"])
        if json_stream is None:
            logger.error(f"""json response bytes stream returned as none with error. Not inserting post {post['id']} \n
            - post {pprint.pprint(post)}
            """)
            continue


        uploaded_screenshot_filepath: str | None = insert_static_file_to_blob(
            memory_buffer=screenshot_stream,
            bucket_name=BUCKET_NAME,
            full_filepath=post['screenshot_path'],
            content_type="image/png",
            minio_client=MINIO_CLIENT
        )

        uploaded_json_filepath: str | None = insert_static_file_to_blob(
            memory_buffer=json_stream,
            bucket_name=BUCKET_NAME,
            full_filepath=post['json_path'],
            content_type="application/json",
            minio_client=MINIO_CLIENT
        )

        if uploaded_screenshot_filepath is None or uploaded_json_filepath is None:
            logger.error(f"Uploaded screenshot or json filepath is None so there was an error in inserting a static file to blob")
            continue
        else:
            logger.info(f"Sucessfully inserted screenshot and json to blob storage: \n - sreenshot: {uploaded_json_filepath} \n -json post: {uploaded_json_filepath}")
        
        post['screenshot_path'] = uploaded_screenshot_filepath
        post['json_path'] = uploaded_json_filepath
        
        post_creation_response: RedditPostCreationResult | None = insert_reddit_post(
            post=post, 
            reddit_user=author_associated_w_posts[post['id']],
            secrets=secrets
        )
        if post_creation_response is None:
            logger.error(f"Error in creating post for {post['id']}. Post was not added to the database")
            return

        logger.info(f"Extracting comments from json post")
        reddit_comments_dict: RedditCommentAttachmentDict = get_comments_from_json(post, json_stream)
        
        logger.info(f"Making request to API to attach reddit comments to post {reddit_comments_dict['reddit_post']['id']}")
        post_comment_attachment_response: RedditCommentAttachmentDict | None = attach_reddit_post_comments(reddit_comments_dict, secrets=secrets)
        if post_comment_attachment_response is None:
            logger.error(f"Error in attaching comments to post {reddit_comments_dict['reddit_post']['id']}")
            return

        logger.info(f"Created and attached reddit post and user \n  - response: {post_creation_response}")

    if next_button_url is None:
        logger.info(f"next url for page {page_url} was extracted to be None. Exiting recursive call")
        return 

    logger.info(f"next url for page {page_url} extracted as {next_button_url} - continuing to call recursively")

    recursive_insert_raw_reddit_post(
        driver=driver, 
        page_url=next_button_url,
        MINIO_CLIENT=MINIO_CLIENT,
        BUCKET_NAME=BUCKET_NAME,
        login=False,
        secrets=secrets
    )
