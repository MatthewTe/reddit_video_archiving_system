from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import pprint
import requests
import io

from reddit_post_extraction_methods import (
    RedditPostDict, RedditPostCreationResult, 
    get_post_dict_from_element, get_post_json, take_post_screenshot, 
    insert_static_file_to_blob, insert_reddit_post
)

if __name__ == "__main__":

    MINIO_CLIENT = Minio("play.min.io",
        access_key="Q3AM3UQ867SPQQA43P2F",
        secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    )
    BUCKET_NAME = "reddit_posts"
    SUBREDDIT = "CombatFootage"

    found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
    if not found:
        MINIO_CLIENT.make_bucket(BUCKET_NAME)
        print("Created bucket", BUCKET_NAME)
    else:
        print("Bucket", BUCKET_NAME, "already exists")



    driver = webdriver.Chrome()
    driver.get(f"https://old.reddit.com/r/{SUBREDDIT}/")

    driver.implicitly_wait(30)

    # Getting all of all reddit posts as an html content:
    posts_site_table = driver.find_element(By.ID, "siteTable")

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

    def insert_raw_reddit_post(driver: webdriver.Chrome, page_url: str):
        driver.get(page_url)

        driver.implicitly_wait(3000)
        posts_site_table = driver.find_element(By.ID, "siteTable")

        all_posts_on_page = posts_site_table.find_elements(By.XPATH, "//div[@data-context='listing']")
        
        posts_to_ingest: list[RedditPostDict] = []

        for post in all_posts_on_page:
            post_dict: RedditPostDict = get_post_dict_from_element(post)
            posts_to_ingest.append(post_dict)

        unique_post_response = requests.get("example_urls", params={'reddit_post_ids': [post['id']for post in posts_to_ingest]})
        unique_post_json: list[dict] = unique_post_response.json()

        duplicate_ids: list[str] = [post["id"] for post in unique_post_json]
        unique_posts: list[RedditPostDict] = [post for post in all_posts_on_page if post['id'] not in duplicate_ids]

        # Actually inserting the post data into the database:
        # TODO: Implement this

        if len(unique_posts) == 0:
            return

        for post in unique_posts:
            screenshot_stream: io.BytesIO | None = take_post_screenshot(driver, post['url'])
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

            # TODO: Do better error handeling here:
            if uploaded_screenshot_filepath is not None:
                post['screenshot_path'] = uploaded_screenshot_filepath
            else:
                continue
            if uploaded_json_filepath is not None:
                post['json_path'] = uploaded_json_filepath
            else:
                continue

            post_creation_response: RedditPostCreationResult | None = insert_reddit_post(post)


        next_button_results: list = driver.find_elements(By.XPATH, "//span[@class='next-button']")
        if len(next_button_results) == 0:
            return
        
        next_button_url = next_button_results[0].find_element(By.TAG_NAME, "a").get_attribute("href")

        insert_raw_reddit_post(driver, next_button_url)

    