from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import argparse
from loguru import logger
import sys

from lib.reddit_post_extraction_methods import recursive_insert_raw_reddit_post
from lib.config import get_secrets, load_config_from_file, Secrets, RedditPipelineConfig
from lib.ingest_reddit_video import ingest_all_video_data

parser = argparse.ArgumentParser()
parser.add_argument("config", help="The full path to the config json file used to run the pipeline")
parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")

args = parser.parse_args()

secrets: Secrets = get_secrets(args.env_file)
reddit_pipeline_configs: RedditPipelineConfig = load_config_from_file(args.config)

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

for subreddit in reddit_pipeline_configs: 
    logger.info(f"Running ingestion pipeline for subreddit {subreddit['Subreddit']}")

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    inserted_reddit_post_ids: list[str]

    if "Ingest_Posts" in subreddit["Operations"]:

        logger.info(f"Ingesting Reddit Posts into Graph db:")

        recursive_insert_raw_reddit_post(
            driver=driver, 
            page_url=f"https://old.reddit.com/r/{subreddit['Subreddit']}/",
            MINIO_CLIENT=MINIO_CLIENT,
            BUCKET_NAME=BUCKET_NAME,
            login=True,
            secrets=secrets
        )

    if "Ingest_Videos" in subreddit["Operations"]:

        logger.info(f"Running full static file ingestion for {len(inserted_reddit_post_ids)} video posts in the neo4j database")

        ingest_all_video_data(secrets, inserted_reddit_post_ids)