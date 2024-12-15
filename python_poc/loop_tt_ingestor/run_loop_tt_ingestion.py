import argparse
from ingest_articles import process_loop_page
from config import LoopPageConfig, Article, get_secrets

parser = argparse.ArgumentParser()
#parser.add_argument("config", help="The full path to the config json file used to run the pipeline")
parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")

args = parser.parse_args()

if __name__ == "__main__":

    config: LoopPageConfig = {
        "query_param_str": '?page=0',
        "article_category": "looptt-crime",
        'db_category': "crime",
        "secrets": get_secrets(args.env_file)
    }

    process_loop_page(config)