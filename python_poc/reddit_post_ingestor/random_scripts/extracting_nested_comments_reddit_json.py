import json
from loguru import logger
import pandas as pd
import pprint
import time

from selenium import webdriver
from selenium.webdriver.common.by import By

# TODO: NOT GOOOD!!!!!

def recursively_pack_comments(comment_json, reddit_url, use_driver_extract_additional: bool) -> dict:

    comment_kind = comment_json['kind']
    if comment_kind == "more" and use_driver_extract_additional:

        for more_comment_id in comment_json['data']['children']:
            additional_comments_url = f"https://www.reddit.com{reddit_url}{more_comment_id}/.json"
            
            driver = webdriver.Chrome()
            driver.implicitly_wait(3000)
            driver.get(additional_comments_url)

            time.sleep(0.5)

            json_element = driver.find_element(By.XPATH, "/html/body/pre")
            json_dict_str =  json_element.get_attribute("innerText")
            additional_full_comment_json = json.loads(json_dict_str)

            time.sleep(0.5)

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

if __name__ == "__main__":

    with open("../example_data/heavy_comments.json", "r") as f:
        post_json = json.load(f)

    first_test_comment = post_json[1]["data"]["children"][0]
    comments = recursively_pack_comments(first_test_comment, post_json[0]['data']['children'][0]['data']["permalink"], False)

    #print(comments)
    with open('./test_output.json', "w") as f:
        json.dump(comments, f)