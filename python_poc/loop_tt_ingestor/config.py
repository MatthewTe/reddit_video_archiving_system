from dotenv import load_dotenv
from typing import TypedDict
from datetime import datetime

import os

class Secrets(TypedDict):
    neo4j_url: str
    neo4j_read_url: str
    neo4j_auth: str

class LoopPageConfig(TypedDict):
    query_param_str: str
    article_category: str
    db_category: str
    secrets: Secrets

class Article(TypedDict):
    id: str
    title: str
    url: str
    type: str
    content: str
    source: str
    extracted_date: str
    published_date: str
    extracted_from: str
    article_category: str

def get_secrets(env_path) -> Secrets:

    load_dotenv(env_path)

    return {
        "neo4j_url": os.environ.get("API_NEO4J_URL"),
        "neo4j_read_url": os.environ.get("GRAPH_READ_URL"),
        "neo4j_auth": os.environ.get("NEO4J_AUTH")
    }