from typing import TypedDict
from dotenv import load_dotenv
import os 

class Secrets(TypedDict):
    minio_url: str
    minio_access_key: str
    minio_secret_key: str
    neo4j_url: str
    reddit_username: str
    reddit_password: str
    neo4j_read_url: str
    neo4j_auth: str

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