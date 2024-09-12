from typing import TypedDict
import uuid
import pandas as pd
import json
import requests
import io
import sys
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
'''
    type RedditPost struct {
        Id               string    `json:"id"`
        Subreddit        string    `json:"subreddit"`
        Url              string    `json:"url"`
        Title            string    `json:"title"`
        StaticDownloaded bool      `json:"static_downloaded_flag"`
        Screenshot       string    `json:"screenshot_path"`
        JsonPost         string    `json:"json_path"`
        CreatedDate      time.Time `json:"created_date"`
        StaticRootUrl    string    `json:"static_root_url"`
        StaticFileType   string    `json:"static_file_type"`
    }
    '''

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

def get_post_json(url) -> io.BytesIO | None:

    time.sleep(1)

    try:
        get_json_response = requests.get(f"{url}.json")
        get_json_response.raise_for_status()

        json_dict = get_json_response.json()

        json_bytes_stream = io.BytesIO()
        json_bytes_stream.write(json.dumps(json_dict).encode())

        return json_bytes_stream

    except requests.HTTPError as err:
        return None

def take_post_screenshot(driver, url) -> bytes:

    driver.get(url)
    driver.implicitly_wait(4000)

    time.sleep(5)

    screenshot_base64 = driver.get_screenshot_as_base64()
    screenshot_bytes: bytes = base64.b64decode(screenshot_base64)

    screenshot_bytes_stream = io.BytesIO()
    screenshot_bytes_stream.write(screenshot_bytes)

    return screenshot_bytes_stream

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

def insert_reddit_post(post: RedditPostDict, reddit_user: RedditUserDict) -> RedditPostCreationResult | None:

    try:
        reddit_post_creation_response = requests.post("http://localhost:8080/v1/reddit/create_post", json=post)
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
        attached_user_respone = requests.post("http://localhost:8080/v1/reddit/attach_user_to_post", json={
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
    

def recursive_insert_raw_reddit_post(driver: webdriver.Chrome, page_url: str, MINIO_CLIENT, BUCKET_NAME: str, login: bool=False):
    driver.get(page_url)

    driver.implicitly_wait(3000)

    # Loging in:
    if login:
        login_button = driver.find_element(By.XPATH, "//a[@class='login-required login-link']")
        login_button.click()

        time.sleep(2)

        login_input = driver.find_element(By.ID, "login-username")
        password_input = driver.find_element(By.ID, "login-password")

        login_input.send_keys("")
        password_input.send_keys("")

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

        author_dict: RedditAuthorDict = get_author_dict_from_element(post_element)
        author_associated_w_posts[post_dict['id']] = author_dict

        posts_to_ingest.append(post_dict)

    logger.info(f"Found a total of {len(posts_to_ingest)} posts from page {page_url}")

    try:
        unique_post_response = requests.get("http://localhost:8080/v1/reddit/check_posts_exists", params={'reddit_post_ids': [post['id']for post in posts_to_ingest]})
        unique_post_response.raise_for_status()
        unique_post_json: list[dict] = unique_post_response.json()
    except requests.HTTPError as exception:
        logger.exception(exception)
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
        logger.info(f"Extracting json representation of post {post['url']}")
        json_stream: io.BytesIO | None = get_post_json(post["url"])
        
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
            return
        
        post['screenshot_path'] = uploaded_screenshot_filepath
        post['json_path'] = uploaded_json_filepath
        
        post_creation_response: RedditPostCreationResult | None = insert_reddit_post(
            post=post, 
            reddit_user=author_associated_w_posts[post['id']]
        )

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
        login=False
    )
