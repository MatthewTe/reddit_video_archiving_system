from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import argparse
from loguru import logger
import sys

from reddit_post_extraction_methods import recursive_insert_raw_reddit_post
from config import get_secrets, Secrets

parser = argparse.ArgumentParser()
parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")
parser.add_argument("subreddit_url", help="The url to start the recursive extraction")

args = parser.parse_args()

print(args.env_file)
print(args.subreddit_url)

secrets: Secrets = get_secrets(args.env_file)

MINIO_CLIENT = Minio(
    secrets['minio_url'],
    access_key=secrets['minio_access_key'],
    secret_key=secrets['minio_secret_key'],
    secure=False
)
BUCKET_NAME = "reddit-posts"

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
    page_url=args.subreddit_url,
    MINIO_CLIENT=MINIO_CLIENT,
    BUCKET_NAME=BUCKET_NAME,
    login=True,
    secrets=secrets
)
