import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import os
import pandas as pd

from utils.static_file_loaders import get_video
from neo4j import GraphDatabase

dash.register_page(__name__, name="Spatially Labeling Static Files")

layout = html.Div([
    dbc.Col([

        dbc.Row([
            html.H4("Static File Type:"),
            dcc.Dropdown(id="node_filter_dropdown", options=['Video', 'Image'], value=None)
        ]),

        dbc.Row([
            dash_table.DataTable(
                id="avalible_static_files_tbl",
                page_current= 0,
                page_size= 10,
            )
        ], style={'padding-top':'1rem'})
    ])
], style={'padding':'1rem'})


@dash.callback(
    Output("avalible_static_files_tbl", "data"),
    Output("avalible_static_files_tbl", "columns"),
    Input("node_filter_dropdown", "value")
)
def query_node_items(static_file_option: str | None) -> tuple[dict, dict]:

    if static_file_option is None:
        raise PreventUpdate

    neo4j_auth_usr, neo4j_auth_pwd = os.environ.get("NEO4J_AUTH").split("/")
    with GraphDatabase.driver(os.environ.get("NEO4J_READ_URL"), auth=(neo4j_auth_usr, neo4j_auth_pwd)) as driver:
        driver.verify_connectivity()

        records, summary, keys = driver.execute_query(
        """
            MATCH (n:StaticFile:Video)
            OPTIONAL MATCH (n)-[r:LOCATED_IN]->(m:SpatialIndex:H3Cell)
            RETURN n,
            COALESCE(m.index, 'None') AS h3_index
        """,
            static_file_type=static_file_option
        )

        rows = []
        for record in records:
            record_data = record.data()
            row = record_data['n']
            row['h3_index'] = record_data['h3_index']
            rows.append(row)

        return rows, [{"name": k, "id": k} for k in rows[0].keys()]

@dash.callback(
    Input("avalible_static_files_tbl", "active_cell"),
    State("avalible_static_files_tbl", "data")
)
def select_video_stream(active_cell, data):
    df = pd.DataFrame.from_records(data)
    active_row = df[df['id'] == active_cell['row_id']]
    get_video(active_row['id'].iloc[0], bucket_name="reddit-posts")
    # Get and parse MPD file - get all of the periods and prepare to stream it:

    pass