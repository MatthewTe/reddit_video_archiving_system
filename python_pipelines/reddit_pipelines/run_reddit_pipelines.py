from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import argparse
from loguru import logger
import json
import sys

from lib.restore_graph_data_from_blob import restore_reddit_post
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

objects = MINIO_CLIENT.list_objects(BUCKET_NAME)
for object in objects:

    post_id = object.object_name.replace("/", "")
    restore_reddit_post(post_id, BUCKET_NAME, MINIO_CLIENT, secrets)

sys.exit(0)