from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import pprint
import requests
from loguru import logger
import io
import sys

from reddit_post_extraction_methods import recursive_insert_raw_reddit_post

if __name__ == "__main__":

    MINIO_CLIENT = Minio(
        "localhost:9000",
        access_key="test_key",
        secret_key="test_password",
        secure=False
    )
    BUCKET_NAME = "reddit-posts"
    SUBREDDIT = "CombatFootage"

    found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
    if not found:
        MINIO_CLIENT.make_bucket(BUCKET_NAME)
        logger.info("Created bucket", BUCKET_NAME)
    else:
        logger.info("Bucket", BUCKET_NAME, "already exists")

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    recursive_insert_raw_reddit_post(
        driver=driver, 
        page_url=f"https://old.reddit.com/r/{SUBREDDIT}/",
        MINIO_CLIENT=MINIO_CLIENT,
        BUCKET_NAME=BUCKET_NAME,
        login=True
    )
    