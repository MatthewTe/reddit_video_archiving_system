import s3fs
import geopandas as gpd
import pandas as pd
import time
import shapely
import h3
from multiprocessing import Pool
import matplotlib.pyplot as plt

fs = s3fs.S3FileSystem(
    anon=False,
    use_ssl=False,
    client_kwargs={
        "endpoint_url": "http://127.0.0.1:9000",
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_secret_key"
    }
)

records = []
for i in range(0, 20):
    start_time = time.time()
    gdf = gpd.read_parquet(
        "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
        filesystem=fs
        # bbox=(-64.739342,10.052167,-64.646301,10.142133)
    )
    end_time = time.time()

    records.append({
        "num_rows": len(gdf),
        "path": "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
        "query_time": end_time - start_time,
        "bbox": None
    })

    print(f"Query {i} for full dataset took {end_time - start_time}")

for i in range(0, 20):
    start_time = time.time()
    gdf = gpd.read_parquet(
        "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
        filesystem=fs,
        bbox=(-64.739342,10.052167,-64.646301,10.142133)
    )
    end_time = time.time()

    records.append({
        "num_rows": len(gdf),
        "path": "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet",
        "query_time": end_time - start_time,
        "bbox": (-64.739342,10.052167,-64.646301,10.142133)
    })

    print(f"Query {i} for bbox dataset took {end_time - start_time}")

df = pd.DataFrame.from_records(records)
df.to_csv("./benchmarking_bbox_query.csv", index=False)
print(df.head())