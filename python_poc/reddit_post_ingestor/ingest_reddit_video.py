from neo4j import GraphDatabase
import neo4j
import json
import argparse
import typing
from minio import Minio

from config import get_secrets, Secrets
from reddit_video.extract_video import RedditVideoInfoDict, extract_video_files_from_reddit_video_dict

def extract_reddit_posts(tx):
    result = tx.run("""
        MATCH (n:Reddit:Post 
            {static_downloaded: false, static_file_type: 'video'})-[:EXTRACTED]->(p:Reddit:Json) 
        RETURN n, p
    """)

    json_posts = []
    for record in result:
        node, json_node = record.values()[0],record.values()[1] 
        json_posts.append({
            "post_id": node['id'],
            "title": node["title"],
            'file_type': node['static_file_type'],
            "json_staticfile_path": json_node['path']
        })

    return json_posts

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")

    args = parser.parse_args()

    secrets: Secrets = get_secrets(args.env_file)
    neo4j_auth_lst = secrets["neo4j_auth"].split("/")
    neo4j_username, neo4j_pwd = neo4j_auth_lst[0], neo4j_auth_lst[1]
    driver: neo4j.Driver = GraphDatabase.driver(secrets['neo4j_read_url'], auth=(neo4j_username, neo4j_pwd))
    with driver.session() as session:
        all_video_posts = session.execute_read(extract_reddit_posts)
    
    MINIO_CLIENT = Minio(
        secrets['minio_url'],
        access_key=secrets['minio_access_key'],
        secret_key=secrets['minio_secret_key'],
        secure=False
    )
    BUCKET_NAME = "reddit-posts"
       
    for video_post in all_video_posts[0:1]:
        try:
            response = MINIO_CLIENT.get_object(BUCKET_NAME, video_post["json_staticfile_path"])
            response_json = response.json()[0]['data']

            post_data = response_json['children'][0]['data']
            
            media_dict = post_data.get("secure_media", None)
            if media_dict is not None:
                reddit_video: RedditVideoInfoDict = media_dict.get("reddit_video", None)
                extract_video_files_from_reddit_video_dict(reddit_video)

        except Exception as e:
            print(str(e))

        finally:
            response.close()
            response.release_conn()