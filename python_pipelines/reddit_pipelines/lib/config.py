from typing import TypedDict
from loguru import logger
from dotenv import load_dotenv
import os 
import json
import pprint

class Secrets(TypedDict):
    minio_url: str
    minio_access_key: str
    minio_secret_key: str
    neo4j_url: str
    reddit_username: str
    reddit_password: str
    neo4j_read_url: str
    neo4j_auth: str

class SubredditIngestionConfig(TypedDict):
    subreddit: str
    operations: list[str]

class RedditPipelineConfig(TypedDict):
    subreddits: list[SubredditIngestionConfig]

def get_secrets(env_path) -> Secrets:

    load_dotenv(env_path)

    return {
        "minio_url": os.environ.get("MINIO_URL"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "neo4j_url": os.environ.get("API_NEO4J_URL"),
        "reddit_username": os.environ.get("REDDIT_USERNAME"),
        "reddit_password": os.environ.get("REDDIT_PASSWORD"),
        "neo4j_read_url": os.environ.get("GRAPH_READ_URL"),
        "neo4j_auth": os.environ.get("NEO4J_AUTH")
    }


def load_config_from_file(config_path: str) -> RedditPipelineConfig:
    with open(config_path, "r") as f:
        config_json = json.load(f)

    reddit_pipeline: RedditPipelineConfig = config_json.get("pipelines").get("reddit")

    logger.info(f"Loaded the subreddit ingestion pipeline from file {config_path}")

    pprint.pprint(config_json)

    return reddit_pipeline