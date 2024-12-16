from datetime import datetime
import pandas as pd
import requests
import argparse

from config import get_secrets, Secrets

parser = argparse.ArgumentParser()
#parser.add_argument("config", help="The full path to the config json file used to run the pipeline")
parser.add_argument("env_file", help="The path to the environment file used to load all of the secrets")

args = parser.parse_args()

secrets: Secrets = get_secrets(args.env_file)

legacy_df = pd.read_csv("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/configs/data/all_tt_articles.csv")

articles_to_insert = []
for index, row in legacy_df.iterrows():

    old_formatted_extracted_date = datetime.strptime(row["extracted_date"], "%B %d, %Y %I:%M %p")
    old_formatted_published_date = datetime.strptime(row["published_date"], "%B %d, %Y %I:%M %p")

    new_formatted_extracted_date = datetime.strftime(old_formatted_extracted_date, "%Y-%m-%d")
    new_formatted_published_date = datetime.strftime(old_formatted_published_date, "%Y-%m-%d")
    
    if row.isnull().any():
        #print(row)
        row["content"] = ""

    payload = {
        "type": "node",
        "query_type": "MERGE",
        "labels": ["Entity", "Article", "LootTT", "Trinidad", "Tobago"],
        "properties": {
            "id": row['id'],
            "url": row['url'],
            "title": row["title"],
            "type": row["type"],
            "content": row["content"],
            "source": row['source'],
            "extracted_date": new_formatted_extracted_date,
            "published_date": new_formatted_published_date
        }
    }

    articles_to_insert.append(payload)

#with open("legacy.json", "w") as f:
#    json.dump(articles_to_insert, f)

#data = json.dumps(articles_to_insert)
inserted_response = requests.post(f"{secrets['neo4j_url']}/v1/api/run_query", json=articles_to_insert)
print(inserted_response.content)
print(inserted_response.json())

