import geopandas as gpd
import json

if __name__ == "__main__":

    gdf: gpd.GeoDataFrame = gpd.read_file("/Users/matthewteelucksingh/Downloads/countries.geojson")
    print(gdf)
    print(gdf.crs)
    
    #gdf.to_parquet(
    #    "/Users/matthewteelucksingh/Downloads/countries.parquet",
    #    write_covering_bbox=True,
    #    geometry_encoding="geoarrow"
    #)
    gdf_2 = gpd.read_parquet("/Users/matthewteelucksingh/Downloads/countries.parquet")

    print(gdf_2)
    print(gdf_2.crs)