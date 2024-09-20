from selenium import webdriver
from selenium.webdriver.common.by import By

import sys
import time
import json
import io
import pandas as pd

driver = webdriver.Chrome()
driver.implicitly_wait(30)

driver.get("https://www.reddit.com/r/CombatFootage/comments/1fh996t/airstrike_on_russian_positions_in_vesoloe_kursk/.json")

time.sleep(2)

json_element = driver.find_element(By.XPATH, "/html/body/pre")

json_dict =  json_element.get_attribute("innerText")
json_bytes_stream = io.BytesIO()
json_bytes_stream.write(json_dict.encode())

json_bytes_stream.seek(0)

row_reddit_json = json.loads(json_bytes_stream.read())

comment_content = row_reddit_json[1]['data']['children']

post_comment_dicts = []
for comment_json in comment_content:

    comment_json = comment_json['data']
    author = comment_json['author']

    if author == '[deleted]':
        associated_user = {
            "author_full_name": "deleted_user",
            "author_name": author
        }
    else:
        associated_user = {
            "author_full_name": comment_json['author_fullname'],
            "author_name": author
        }

    reddit_comment_dict = {
        "reddit_post_id":'id',
        "comment_id":comment_json["id"],
        "comment_body":comment_json["body"],
        "associated_user": associated_user,
        "posted_timestamp": pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="ms").strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    print(comment_json['created_utc'], pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ"))
    post_comment_dicts.append(reddit_comment_dict)

attached_reddit_comment = {
    "reddit_post": "Post",
    "attached_comments": post_comment_dicts
}
