import json
from loguru import logger
import pandas as pd
import pprint
import uuid
import random
import time
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By

# TODO: NOT GOOOD!!!!!

def extract_author_from_json(comment_data: dict) -> dict:
    try:
        author = comment_data['author']
    except Exception as e:
        logger.warning(f"Unable to extract author from post json so setting author to none")
        author = "not_found"

    if author == '[deleted]':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "deleted_user")),
            "author_full_name": "deleted_user",
            "author_name": author
        }
    elif author == 'not_found':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            "author_full_name": "not_found",
            "author_name": author
        }
    else:
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, comment_data['author_fullname'])),
            "author_full_name": comment_data['author_fullname'],
            "author_name": author
        }

    return associated_user

def recursively_pack_comments(comment_json, reddit_url, use_driver_extract_additional: bool) -> dict:

    comment_kind = comment_json['kind']
    if comment_kind == "more" and use_driver_extract_additional:

        for more_comment_id in comment_json['data']['children']:
            additional_comments_url = f"https://www.reddit.com{reddit_url}{more_comment_id}/.json"
            
            driver = webdriver.Chrome()
            driver.implicitly_wait(3000)
            driver.get(additional_comments_url)

            time.sleep(random.uniform(0.04, 0.65))

            json_element = driver.find_element(By.XPATH, "/html/body/pre")
            json_dict_str =  json_element.get_attribute("innerText")
            additional_full_comment_json = json.loads(json_dict_str)

            time.sleep(random.uniform(0.04, 0.65))

            driver.quit()

            comments = additional_full_comment_json[1]['data']['children']
            for comment_json in comments:
                if comment_json["kind"] != "more":
                    recursively_pack_comments(comment_json, reddit_url)
    else:
        comment_json = comment_json['data']

        try:
            author = comment_json['author']
        except Exception as e:
            logger.warning(f"Unable to extract author from post json so setting author to none")
            author = "not_found"

        if author == '[deleted]':
            associated_user = {
                "author_full_name": "deleted_user",
                "author_name": author
            }
        elif author == 'not_found':
            associated_user = {
                "author_full_name": "not_found",
                "author_name": author
            }
        else:
            associated_user = {
                "author_full_name": comment_json['author_fullname'],
                "author_name": author
            }

        comment_replies = comment_json.get("replies", [])
        if len(comment_replies) == 0:

            ## THIS IS HAPPENING THROWING ERRORS BECAUSE OF THE 'SHOW MORE COMMENTS' BUTTON
            ## I can get around this by making inidividual json requests to get the rest of the
            ## comment objects from the webpage and scraping them recrusivley. Is this worth it/
            ## possible?

            ## https://www.reddit.com/r/CombatFootage/comments/ybdldt/comment/itgtcoo/.json
            return []

        else:
            comment_replies = comment_replies["data"]['children']

            parent_comment_dict = {
                "reddit_post_id":"example_post_id",
                "comment_id":comment_json["id"],
                "comment_body":comment_json["body"],
                "associated_user": associated_user,
                "posted_timestamp": pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ"),
                "replies":[recursively_pack_comments(comment_dict, reddit_url, use_driver_extract_additional) for comment_dict in comment_replies]
            }

            return parent_comment_dict

def recursivley_build_comments_graph(post, comment_json_obj, request_node_lst: list[dict]) -> list[dict]:

    if comment_json_obj["kind"] == "more":
        logger.warning("Comment object is a 'more' object. Exiting recusrion for comment tree")
        return request_node_lst
    
    comment_data = comment_json_obj["data"]
    try:
        author = comment_data['author']
    except Exception as e:
        logger.warning(f"Unable to extract author from post json so setting author to none")
        author = "not_found"

    if author == '[deleted]':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "deleted_user")),
            "author_full_name": "deleted_user",
            "author_name": author
        }
    elif author == 'not_found':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            "author_full_name": "not_found",
            "author_name": author
        }
    else:
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, comment_data['author_fullname'])),
            "author_full_name": comment_data['author_fullname'],
            "author_name": author
        }

    # Building all of the nodes:
    author_node = {
        "type":"node",
        "query_type":"MERGE",
        "labels":["Reddit", "User", "Entity", "Account"],
        "properties": {
            "id": associated_user["id"],
            "author_name": associated_user["author_name"],
            "author_full_name": associated_user["author_full_name"],
        }
    }

    comment_full_id: str = str(uuid.uuid3(uuid.NAMESPACE_URL, comment_data["id"]))
    comment_node = {
        "type":"node",
        "query_type":"MERGE",
        "labels":["Reddit", "Entity", "Comment"],
        "properties": {
            "id": comment_full_id,
            "body": comment_data["body"],
            "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    }

    # Edges:
    author_post_comment_edge = {
        "type":"edge",
        "labels": ["POSTED"],
        "connection": {
            "from": comment_full_id, 
            "to": post["id"]
        },
        "properties": {
            "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    }

    print(comment_data["id"])

    replies = comment_data["replies"]
    print(replies)

def recursively_build_comment_v3(post, comment_json_obj, parent_object: dict = None):

    comment_listing_type = comment_json_obj["kind"]

    if comment_listing_type == "t1":
        comment_data = comment_json_obj["data"]
        associated_author = extract_author_from_json(comment_json_obj)

        comment_full_id: str = comment_data["id"]
        comment_node = {
            "type":"node",
            "query_type":"MERGE",
            "labels":["Reddit", "Entity", "Comment"],
            "properties": {
                "id": comment_full_id,
                "body": comment_data["body"],
                "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }

        author_node = {
            "type":"node",
            "query_type":"MERGE",
            "labels":["Reddit", "User", "Entity", "Account"],
            "properties": {
                "id": associated_author["id"],
                "author_name": associated_author["author_name"],
                "author_full_name": associated_author["author_full_name"],
            }
        }

        author_post_comment_edge = {
            "type":"edge",
            "labels": ["POSTED"],
            "connection": {
                "from": associated_author["id"],
                "to": comment_full_id
            },
            "properties": {
                "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }
        comment_to_post_edge = {
            "type":"edge",
            "labels":["COMMENTED_ON"],
            "connection": {
                "from": comment_full_id,
                "to": post['id']
            },
            "properties": {
                "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }
        test_lst.append(comment_node)
        test_lst.append(author_node)
        test_lst.append(author_post_comment_edge)
        test_lst.append(comment_to_post_edge)

        # If this is a recursive call that has provided a parent comment node (it is a reply) connect the current comment to the parent comment:
        if parent_object is not None:
            reply_to_parent_comment_edge = {
                "type":"edge",
                "query_type":"MERGE",
                "labels":["REPLIED_TO"],
                "connection": {
                    "from": comment_full_id,
                    "to": parent_object["properties"]["id"]
                },
                "properties": {
                    "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
            parent_to_reply_comment_edge = {
                "type":"edge",
                "query_type":"MERGE",
                "labels":["HAS_REPLY"],
                "connection": {
                    "from": parent_object["properties"]["id"],
                    "to": comment_full_id
                },
                "properties": {
                    "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
            test_lst.append(reply_to_parent_comment_edge)
            test_lst.append(parent_to_reply_comment_edge)

        replies = comment_data.get("replies", None)
        if isinstance(replies, dict):
            if replies['kind'] != "more":
                for reply in replies["data"]['children']:
                    recursively_build_comment_v3(post, reply, parent_object=comment_node)
        else:
            return

    elif comment_listing_type == "more":
        logger.warning("Recursive post extraction has hit the 'more' component. Exiting recursive extraction.")
        return

if __name__ == "__main__":

    with open("../example_data/heavy_comments.json", "r") as f:
        post_json = json.load(f)

    first_test_comment = post_json[1]["data"]["children"][0]

    test_lst = [
         {
            "type":"node",
            "query_type":"CREATE",
            "labels": ['Entity', 'Post', "Reddit"],
            "properties": {
                "id": "test_id",
                "url": "test_url",
                "title": "This is an example title",
                "static_root_url": "test_id/",
                "static_downloaded":False,
                "static_file_type": None
            }
        },
    ]
    recursively_build_comment_v3({"id":"test_id"}, first_test_comment)

    with open("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/python_poc/reddit_post_ingestor/example_data/example_single_post_recursion_output.json", "w") as f:
        json.dump(test_lst, f)

    #for entry in test_lst:
    #    print(entry)
    #    print("\n")

    requests.post("http://localhost:8080/v1/api/run_query", json=test_lst)