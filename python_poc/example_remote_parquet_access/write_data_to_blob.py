import s3fs
import geopandas as gpd
import time
import shapely
import h3
import sys
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
from multiprocessing import Pool
import matplotlib.pyplot as plt

road_table = pq.read_table(
    "/Users/matthewteelucksingh/Repos/reddit_video_archiving_system/python_poc/example_remote_parquet_access/all_roads_tt.parquet", 
    columns=['geometry', 'name']
)
feather.write_feather(road_table, "all_roads_tt.feather", compression="uncompressed")
sys.exit(0)

fs = s3fs.S3FileSystem(
    anon=False,
    use_ssl=False,
    client_kwargs={
        "endpoint_url": "http://127.0.0.1:9000",
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_secret_key"
    }
)

gdf: gpd.GeoDataFrame = gpd.read_file("/Users/matthewteelucksingh/Downloads/hotosm_tto_roads_lines_shp/hotosm_tto_roads_lines_shp.shp")
gdf.to_parquet("test-bucket/all_roads_tt.parquet", write_covering_bbox=True, geometry_encoding="geoarrow", filesystem=fs)
print(gdf)

sys.exit(0)
#gdf = gpd.read_parquet(
#    'sentinel-2-data/sentinel_2_metadata/uploaded_sentinel_2_footprints.parquet',
#    filesystem=fs
    #bbox=(-14.5152, 7.5413, 173.9021, 63.5346)
#)
#print(gdf)

def generate_hex_from_bbox(bbox: tuple) -> gpd.GeoDataFrame:

    bbox_poly = shapely.geometry.box(*bbox)
    h3_poly = h3.geo_to_h3shape(bbox_poly)
    h3_idx: list[str] = h3.polygon_to_cells(h3_poly, 9)

    print("Generated")

    rows = []
    for idx in h3_idx:
        rows.append({
            "h3_id": idx,
            "geometry": shapely.Polygon(h3.cell_to_boundary(idx))
        })

    print(f"Added {len(rows)} h3 cells from bbox {bbox}")

    gdf = gpd.GeoDataFrame.from_records(rows)
    gdf = gdf.set_geometry('geometry')
    gdf.set_crs(4326)

    return gdf

gdf: gpd.GeoDataFrame = generate_hex_from_bbox((-73.3, 0.72, -59.76, 12.16))
gdf.to_parquet(
    "sentinel-2-data/sentinel_2_metadata/venezuela_h3_index.parquet", 
    write_covering_bbox=True,
    geometry_encoding='geoarrow',
    filesystem=fs
)
print(gdf)