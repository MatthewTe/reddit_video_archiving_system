import json
from pathlib import Path
import pandas as pd
from io import BytesIO
import pprint
import sys

if __name__ == "__main__":

    test_json_path = Path("/Users/matthewteelucksingh/Downloads/post.json")

    with open(test_json_path, "r") as f:
        post_json = json.load(f)

    json_stream = BytesIO()
    json_stream.write(json.dumps(post_json).encode())
    json_stream.seek(0)
    
    post_json = json.loads(json_stream.read())

    '''
    type RedditComment struct {
        RedditPostId    string     `json:"reddit_post_id"`
        CommentId       string     `json:"comment_id"`
        Body            string     `json:"comment_body"`
        AssociatedUser  RedditUser `json:"associated_user"`
        PostedTimestamp time.Time  `json:"posted_timestamp"`
    }       

    type RedditUser struct {
	AuthorName     string `json:"author_name"`
	AuthorFullName string `json:"author_full_name"`
    }

    '''
    comment_content = post_json[1]['data']['children']
    for comment_json in comment_content:

        comment_json = comment_json['data']

        try:
            author = comment_json["author"]
            if author == '[deleted]':
                associated_user = {
                    "author_fullname":"deleted_user",
                    "author":author
                }
            else:
                associated_user = {
                    "author_fullname":comment_json["author_fullname"],
                    "author":author
                }

            content = {
                "reddit_post_id":"Test_id",
                "comment_id":comment_json["id"],
                "comment_body":comment_json["body"],
                "associated_user": associated_user,
                "posted_timestamp": pd.to_datetime(int(comment_json['created_utc']), utc=True, unit="ms").strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            pprint.pprint(content['associated_user'])

        except Exception as e:
            print("ERRROR")
            pprint.pprint(comment_json)
