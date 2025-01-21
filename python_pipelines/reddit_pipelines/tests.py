import io
import json

from loguru import logger
from lib.reddit_post_extraction_methods import RedditCommentAttachmentDict, get_comments_from_json

def test_comment_extraction_ingestion():

    with open("./test_data/reddit_post.json", "r") as f:
        parsed_reddit_post = json.load(f)

    logger.info(f"Loaded parsed post object for testing")

    with open("./test_data/raw_comments.json", "r") as f:
        full_post_stream = io.BytesIO(f.read().encode())
    logger.info(f"Loaded comment stream for testing")

    full_post_stream.seek(0)

    reddit_comments_dict: RedditCommentAttachmentDict = get_comments_from_json(
        parsed_reddit_post, 
        full_post_stream
    )

    logger.info(f"Parsed comments from post and json body {reddit_comments_dict}")
    
if __name__ == "__main__":

    test_comment_extraction_ingestion()
    pass