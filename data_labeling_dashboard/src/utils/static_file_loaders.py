from minio import Minio
import os
import io

from dotenv import load_dotenv
import xml.etree.ElementTree as ET 

def get_video(mpd_file_path: str, bucket_name: str) -> io.BytesIO:
    
    client = Minio(
        os.environ.get("MINIO_URL"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=False
    )

    mpd_file_response = client.get_object(
        bucket_name=bucket_name,
        object_name=mpd_file_path
    )

    mpd_stream = io.BytesIO(mpd_file_response.data) 
    mpd_stream.seek(0)

    mpd_file_response.close()
    mpd_file_response.release_conn()

    root = ET.fromstring(mpd_stream.read())


    print(root)

    for child in root.findall("./period"):
        print(child.attrib)
    # Get all periods
    # For each period get the base url of the video component
    
if __name__ == "__main__":
    load_dotenv("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/configs/prod/prod.env")

    get_video(
        mpd_file_path="00042d7e-60a4-3b71-9174-9556001c5572/Graph_DASH.mpd",
        bucket_name="reddit-posts"
    )
