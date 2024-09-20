import json
from loguru import logger
import pandas as pd
import pprint

# TODO: NOT GOOOD!!!!!

def parse_comment_dict(comment: dict):

    comment_json = comment['data']

    try:
        author = comment_json['author']
    except Exception as e:
        logger.warning(f"Unable to extract author from post json so setting author to none: \n {pprint.pprint(comment_json)}")
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

    comment_replies = comment_json.get("replies", None)

    # Extracting the replies that are nested:
    try:
        reddit_comment_dict = {
            "reddit_post_id":"example_post_id",
            "comment_id":comment_json["id"],
            "comment_body":comment_json["body"],
            "associated_user": associated_user,
            "posted_timestamp": pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "replies":comment_replies
        }

        recursivley_extract_comment_replies(reddit_comment_dict)

    except Exception as e:
        logger.warning(str(e))

    pass

def recursivley_extract_comment_replies(parsed_comment: dict):

    if parsed_comment['replies'] is None:
        return
    
    parsed_comment['num_replies'] = len(parsed_comment['replies']['data']['children'])

    pprint.pprint(parsed_comment)

    parsed_comment_replies = parsed_comment['replies']['data']['children']
    for comment in parsed_comment_replies:
        parse_comment_dict(comment)



if __name__ == "__main__":

    with open("../example_data/heavy_comments.json", "r") as f:
        post_json = json.load(f)


    comment_children = post_json[1]['data']['children']

    for comment in comment_children[0:1]:
        parse_comment_dict(comment)