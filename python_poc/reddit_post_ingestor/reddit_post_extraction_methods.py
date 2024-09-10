from typing import TypedDict
import uuid
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By

'''
    type RedditPost struct {
        Id               string    `json:"id"`
        Subreddit        string    `json:"subreddit"`
        Url              string    `json:"url"`
        Title            string    `json:"title"`
        StaticDownloaded bool      `json:"static_downloaded_flag"`
        Screenshot       string    `json:"screenshot_path"`
        JsonPost         string    `json:"json_path"`
        CreatedDate      time.Time `json:"created_date"`
        StaticRootUrl    string    `json:"static_root_url"`
        StaticFileType   string    `json:"static_file_type"`
    }
    '''

class RedditPostDict(TypedDict):
    id: str
    subreddit: str
    url: str
    title: str
    static_downloaded_flag: bool
    screenshot_path: str 
    json_path: str
    created_date: str
    static_root_url: str
    static_file_type: str
def get_post_dict_from_element(post_element) -> RedditPostDict:

    reddit_post_id = post_element.get_attribute("id")
    title = post_element.find_element(By.CSS_SELECTOR, f"#{reddit_post_id} > div:nth-child(5) > div:nth-child(1) > p:nth-child(1) > a:nth-child(1)").text
    id = str(uuid.uuid3(namespace=uuid.NAMESPACE_DNS, name=reddit_post_id))

    subreddit = post_element.get_attribute("data-subreddit")
    url = f"https://www.reddit.com{post_element.get_attribute('data-permalink')}"

    static_downloaded = False
    screenshot = f"{id}/screenshot.png"
    json = f"{id}/post.json"

    post_unix_timestamp = post_element.get_attribute("data-timestamp")
    created_date = pd.to_datetime(int(post_unix_timestamp), utc=True, unit="ms").strftime("%Y-%m-%dT%H:%M:%SZ")

    static_root_url = f"{id}/"
    static_file_type = post_element.get_attribute(f"data-kind")

    post_data_dict: RedditPostDict = {
        "id": id,
        "subreddit": subreddit,
        "url": url,
        "title": title,
        "static_downloaded_flag": static_downloaded,
        "screenshot_path": screenshot,
        "json_path": json,
        "created_date": created_date,
        "static_root_url": static_root_url,
        "static_file_type": static_file_type
    }

    return post_data_dict