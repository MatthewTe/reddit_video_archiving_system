import openeo

# https://open-eo.github.io/openeo-python-client/data_access.html

connection = openeo.connect('openeo.dataspace.copernicus.eu')

#collection_ids = connection.list_collection_ids()
connection.authenticate_oidc()

s2a_description = connection.describe_collection('SENTINEL2_L2A')

sentinel2_cube = connection.load_collection(
    'SENTINEL2_L2A',
    spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19},
    temporal_extent = ["2021-02-01", "2021-04-30"],
    bands=["B04", "B03", "B02"]
)

sentinel2_cube.download('text_date.tiff')