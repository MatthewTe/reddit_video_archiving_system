import pyarrow
import s3fs
from pyarrow import parquet
import geopandas as gpd
import json

fs = s3fs.S3FileSystem(
    anon=False,
    use_ssl=False,
    client_kwargs={
        "endpoint_url": "http://127.0.0.1:9000",
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_secret_key"
    }
)

tbl = parquet.ParquetDataset(
    "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
    filesystem=fs
)

print(tbl.schema)

metadata = parquet.read_metadata(
    "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
    filesystem=fs
).metadata

print("\n")

print(json.loads(metadata.get(b"geo").decode("utf-8")))

#gdf = gpd.read_parquet(
#    "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
#    filesystem=fs,
#    bbox=(10.052167, -64.739342, 10.142133, -64.646301)
#)

#print(gdf)