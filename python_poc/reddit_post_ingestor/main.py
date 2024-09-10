from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from datetime import datetime, timezone
import pandas as pd
import pprint
import requests

from reddit_post_extraction_methods import RedditPostDict, get_post_dict_from_element, get_post_json, take_post_screenshot

if __name__ == "__main__":

    SUBREDDIT = "CombatFootage"

    driver = webdriver.Chrome()
    driver.get(f"https://old.reddit.com/r/{SUBREDDIT}/")

    driver.implicitly_wait(30)

    # Getting all of all reddit posts as an html content:
    posts_site_table = driver.find_element(By.ID, "siteTable")

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

    def insert_raw_reddit_post(driver: webdriver.Chrome, page_url: str):
        driver.get(page_url)

        driver.implicitly_wait(3000)
        posts_site_table = driver.find_element(By.ID, "siteTable")

        all_posts_on_page = posts_site_table.find_elements(By.XPATH, "//div[@data-context='listing']")
        
        posts_to_ingest: list[RedditPostDict] = []

        for post in all_posts_on_page:
            post_dict: RedditPostDict = get_post_dict_from_element(post)
            posts_to_ingest.append(post_dict)

        unique_post_response = requests.get("example_urls", params={'reddit_post_ids': [post['id']for post in posts_to_ingest]})
        unique_post_json: list[dict] = unique_post_response.json()

        duplicate_ids: list[str] = [post["id"] for post in unique_post_json]
        unique_posts: list[RedditPostDict] = [post for post in all_posts_on_page if post['id'] not in duplicate_ids]

        # Actually inserting the post data into the database:
        # TODO: Implement this

        if len(unique_posts) == 0:
            return


        next_button_url = driver.find_elements(By.XPATH, "//span[@class='next-button']").find_element(By.TAG_NAME, "a").get_attribute("href")
        if len(next_button_url) == 0:
            return
        
        insert_raw_reddit_post(driver, next_button_url)

    