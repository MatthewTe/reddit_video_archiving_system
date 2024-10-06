from neo4j import GraphDatabase
import neo4j
import json
import argparse
import typing
import pprint
import requests
import io
import time
import random
import os
from minio import Minio
from loguru import logger
import uuid
import xml.etree.ElementTree as ET

from config import get_secrets, Secrets
from reddit_video.extract_video import RedditVideoInfoDict, parse_video_from_mpd_document, ParsedMPDResult

def extract_reddit_posts(tx):
    result = tx.run("""
        MATCH (n:Reddit:Post 
            {static_downloaded: false, static_file_type: 'video'})-[:EXTRACTED]->(p:Reddit:Json) 
        RETURN n, p
        LIMIT 1
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
       
    for video_post in all_video_posts:

        time.sleep(random.randint(2, 4))

        logger.info(f"Starting to parse reddit video from node with id {video_post['post_id']}")

        try:
            response = MINIO_CLIENT.get_object(BUCKET_NAME, video_post["json_staticfile_path"])
            response_json = response.json()[0]['data']

            post_data = response_json['children'][0]['data']
            
            media_dict = post_data.get("secure_media", None)
            if media_dict is not None:
                reddit_video: RedditVideoInfoDict = media_dict.get("reddit_video", None)
                parsed_mpd_result: ParsedMPDResult = parse_video_from_mpd_document(reddit_video, post_data)

                logger.info(f"Successfully parsed the mpd result with {len(parsed_mpd_result['videos_periods'].keys())} periods")

                mpd_file_byte_stream = io.BytesIO(parsed_mpd_result['mpd_file'].encode("UTF-8"))

                MINIO_CLIENT.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=f"{video_post['post_id']}/Origin_DASH.mpd",
                    data=mpd_file_byte_stream,
                    length=mpd_file_byte_stream.getbuffer().nbytes,
                    content_type="application/dash+xml"
                )

                # Creating the new MDP file for the uploaded content:
                mpd_ns = "urn:mpeg:dash:schema:mpd:2011"
                xsi_ns = "http://www.w3.org/2001/XMLSchema-instance"
                ET.register_namespace('', mpd_ns)
                ET.register_namespace('xsi', xsi_ns)
                mpd = ET.Element("MPD", {
                    "xmlns": mpd_ns,
                    "xmlns:xsi": xsi_ns,
                    "profiles": "urn:mpeg:dash:profile:isoff-on-demand:2011",
                    "type": "static",
                    "xsi:schemaLocation": "urn:mpeg:dash:schema:mpd:2011 DASH-MPD.xsd"
                })

                # Uploading video files:
                for period_id, video_period in parsed_mpd_result["videos_periods"].items():

                    period = ET.SubElement(mpd, "Period", {
                        "id": str(period_id)
                    })

                    video_period_filename = f"{video_post['post_id']}/{period_id}_{video_period['extension']}"
                    video_period['video_byte_stream'].seek(0)

                    # Static File Uploads:
                    MINIO_CLIENT.put_object(
                        bucket_name=BUCKET_NAME,
                        object_name=video_period_filename,
                        data=video_period['video_byte_stream'],
                        length=video_period['video_byte_stream'].getbuffer().nbytes,
                        content_type=video_period['mime_type']
                    )

                    logger.info(f"Uploaded video file to blob at {video_period_filename}")

                    video_adaptation_set = ET.SubElement(period, "AdaptationSet", {
                        "contentType": "video",
                        "id": str(period_id),
                    })
                    
                    representation = ET.SubElement(video_adaptation_set, "Representation", {
                        "id": str(period_id),
                        "mimeType": video_period['mime_type'],
                    })
                    base_url = ET.SubElement(representation, "BaseURL")
                    base_url.text = video_period_filename

                    # Checking to see if video in this period has accompanying audio:
                    audio_period = parsed_mpd_result["audio_periods"].get(period_id, None)
                    if audio_period is not None:
                        logger.info(f"Extracting audio stream for video in period {period_id}")
                        audio_period_filename = f"{video_post['post_id']}/{period_id}-{audio_period['extension']}"
                        audio_period['audio_byte_stream'].seek(0)

                        # Static File Uploads:
                        MINIO_CLIENT.put_object(
                            bucket_name=BUCKET_NAME,
                            object_name=audio_period_filename,
                            data=audio_period['audio_byte_stream'],
                            length=audio_period['audio_byte_stream'].getbuffer().nbytes,
                            content_type=audio_period['mime_type']
                        )

                        logger.info(f"Uploaded audio file to blob at {audio_period_filename}")

                        audio_adaptation_set = ET.SubElement(period, "AdaptationSet", {
                            "contentType": "audio",
                            "id": str(period_id),
                        })
                        
                        audio_representation = ET.SubElement(audio_adaptation_set , "Representation", {
                            "id": str(period_id),
                            "mimeType": audio_period['mime_type'],
                        })
                        base_url = ET.SubElement(audio_representation, "BaseURL")
                        base_url.text = audio_period_filename

                new_mpd_file = ET.tostring(mpd, encoding="unicode", method="xml")
                new_mpd_file_byte_stream = io.BytesIO(new_mpd_file.encode())
                logger.info("Built new MPD file referencing uploaded video files")

                MINIO_CLIENT.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=f"{video_post['post_id']}/Graph_DASH.mpd",
                    data=new_mpd_file_byte_stream ,
                    length=new_mpd_file_byte_stream .getbuffer().nbytes,
                    content_type="application/dash+xml"
                )
                logger.info(f"Uploaded {video_post['post_id']}/Graph_DASH.mpd")


                # Creating the Video Ingestion Node and Edge to the Graph database:
                neo4j_node_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f"{video_post['post_id']}/Origin_DASH.mpd"))
                video_node_w_connection = [
                    {
                        "type":"node",
                        "query_type":"MERGE",
                        "labels": ['Reddit', 'StaticFile', 'Video'],
                        "properties": {
                            "id": neo4j_node_id,
                            "mpd_file": f"{video_post['post_id']}/Graph_DASH.mpd"
                        }
                    },
                    {
                        "type":"edge",
                        "labels": ['CONTAINS'],
                        "connection": {
                            "from":neo4j_node_id,
                            "to": video_post['post_id']
                        },
                        "properties": {}
                    },
                    {
                        "type":"edge",
                        "labels": ['EXTRACTED_FROM'],
                        "connection": {
                            "from": video_post['post_id'],
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

                update_video_node = [
                    {
                        "type":"node",
                        "query_type":"MATCH",
                        "labels": ["Reddit", "Post", "Entity"],
                        "match_properties": {
                            "id": video_post["post_id"]
                        },
                        "set_properties": {
                            "static_downloaded": True
                        }
                    }
                ]

                node_updated_response = requests.post(
                    f"{os.environ.get('NEO4J_URL')}/v1/api/run_update_query",
                    json=update_video_node
                )
                node_updated_response.raise_for_status()
                logger.info(f"Updated the Reddit node to {video_post['post_id']}. Setting static file to True")
                pprint.pprint(node_updated_response.json())
                
        except Exception as e:
            logger.error(str(e.with_traceback(None)))

        finally:
            response.close()
            response.release_conn()