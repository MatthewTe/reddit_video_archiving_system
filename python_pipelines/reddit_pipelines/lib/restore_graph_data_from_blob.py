import requests
from loguru import logger
import pprint
import io
import os
import json
import pandas as pd

import uuid
from lib.config import Secrets
from .reddit_post_extraction_methods import (
    RedditPostDict, 
    RedditUserDict, 
    RedditCommentAttachmentDict, 
    RedditPostCreationResult,
    insert_reddit_post,
    get_comments_from_json,
    attach_reddit_post_comments
)

def extract_reddit_post_from_json(json_dict: dict, id: str) -> list[RedditPostDict, RedditUserDict]:
    
    root = json_dict[0]['data']['children'][0]['data']

    post_unix_timestamp = root['created_utc']
    created_date = pd.to_datetime(int(post_unix_timestamp), utc=True, unit="ms").strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        if "hosted:video" in root['post_hint']:
            file_type = "video"
        else:
            file_type = "unknown"
    except:
        file_type = "unknown"

    post = {
        "id": id,
        "subreddit": root['subreddit'],
        "url": f"https://www.reddit.com{root['permalink']}",
        "title": root['title'],
        "static_downloaded_flag": True,
        "screenshot_path": None,
        "json_path": None,
        "created_date": created_date,
        "static_root_url": f"{id}/",
        "static_file_type": file_type
    }

    try:
        author = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, root["author_fullname"])),
            "author_name": root["author"],
            "author_full_name": root["author_fullname"]
        }
    except Exception as e:

        author = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            "author_full_name": "not_found",
            "author_name": "not_found"
        } 

    return [post, author]


def restore_reddit_post(node_id: str, BUCKET_NAME: str, MINIO_CLIENT, secrets: Secrets):

    if len(node_id) < 2:
        return

    try:
        current_post_ids = ",".join([node_id])
        unique_post_response = requests.get(f"{secrets['neo4j_url']}/v1/api/exists", params={'post_ids': current_post_ids})
        unique_post_response.raise_for_status()
        unique_post_json: dict = unique_post_response.json()[0]
        logger.info(f"Response from unique query: \n")
        pprint.pprint(unique_post_json)

    except requests.HTTPError as exception:
        logger.exception(f"Error in checking existing reddit posts from API {exception}")
        return

    if unique_post_json['exists']:
        logger.warning(f"Post {node_id} exists in blob and in graph. Skipping....")
        return

    logger.info(f"Post {node_id} is in blob storage but not in the graph db - restoring:")

    json_response = MINIO_CLIENT.get_object(BUCKET_NAME, f"{node_id}/post.json")
    json_stream = io.BytesIO(json_response.read())
    json_stream.seek(0)

    post_json = json.loads(json_stream.read())
    json_stream.seek(0)

    post, author = extract_reddit_post_from_json(post_json, node_id)

    # Hardcode this - it should be set already:
    post['screenshot_path'] = f"{node_id}/screenshot.png"
    post['json_path'] = f"{node_id}/post.json"

    post_creation_response: RedditPostCreationResult | None = insert_reddit_post(
        post=post, 
        reddit_user=author,
        secrets=secrets
    )
    if post_creation_response is None:
        logger.error(f"Error in creating post for {post['id']}. Post was not added to the database")
        return
    
    logger.info(f"Extracting comments from json post")
    reddit_comments_dict: RedditCommentAttachmentDict = get_comments_from_json(post, json_stream)
    
    logger.info(f"Making request to API to attach reddit comments to post. Generated json request with {len(reddit_comments_dict)} items")
    post_comment_attachment_response: RedditCommentAttachmentDict | None = attach_reddit_post_comments(reddit_comments_dict, secrets=secrets)
    if post_comment_attachment_response is None:
        logger.error(f"Error in attaching comments to post")
        return
    
    logger.warning(f"Inserting static file content for existing node {post['id']}")

    try:
        MINIO_CLIENT.stat_object(BUCKET_NAME, f"{post['id']}/Origin_DASH.mpd")
        video_file_exists = True
        logger.warning(f"{post['id']} is a video post")
    except:
        logger.warning(f"{post['id']} is NOT a video post")
        video_file_exists =  False

    if video_file_exists:
        neo4j_node_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f"{post['id']}/Origin_DASH.mpd"))
        video_node_w_connection = [
            {
                "type":"node",
                "query_type":"MERGE",
                "labels": ['Reddit', 'StaticFile', 'Video'],
                "properties": {
                    "id": neo4j_node_id,
                    "mpd_file": f"{post['id']}/Graph_DASH.mpd"
                }
            },
            {
                "type":"edge",
                "labels": ['CONTAINS'],
                "connection": {
                    "from":neo4j_node_id,
                    "to": post['id']
                },
                "properties": {}
            },
            {
                "type":"edge",
                "labels": ['EXTRACTED_FROM'],
                "connection": {
                    "from": post['id'],
                    "to": neo4j_node_id
                },
                "properties": {}
            }
        ]

        node_created_response = requests.post(
            f"{os.environ.get('NEO4J_URL')}/v1/api/run_query", 
            json=video_node_w_connection
        )
        node_created_response.raise_for_status()
        logger.info("Created the video node and the edges for the Graph Database")
        pprint.pprint(node_created_response.json())              

