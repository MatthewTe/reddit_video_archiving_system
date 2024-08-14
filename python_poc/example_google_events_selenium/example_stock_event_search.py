from run_search_query import ingest_search_results

from selenium import webdriver
import sqlalchemy as sa
import pandas as pd
from datetime import datetime
import sys

if __name__ == '__main__':

    sqlite_engine = sa.create_engine("sqlite:///stock_news_searches.sqlite")
    with sqlite_engine.connect() as conn, conn.begin():
        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_queries (
                id TEXT,
                full_query_id TEXT,
                start_date TEXT,
                end_date TEXT,
                query TEXT,
                runtime TEXT
            )
            """
        ))

        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_results (
                id TEXT,
                query_id TEXT,
                full_query_id TEXT,
                title TEXT,
                url TEXT,
                published_date TEXT,
                modal TEXT
            )
            """
        ))

    # Date range:
    weekly_date_range = pd.date_range(start="2021-01-01", end="2022-01-01", periods=52).to_pydatetime().tolist()
    weekly_date_range_strs = [date.strftime("%Y-%m-%d") for date in weekly_date_range]

    start_end_intervals: list[dict] = []
    index = 0
    for date_str in weekly_date_range_strs:
        index +=1

        if index < len(weekly_date_range_strs):

            start_date = date_str
            end_date = weekly_date_range_strs[index]

            start_end_intervals.append({"after":start_date, "before":end_date})

    driver = webdriver.Chrome()
    for date_range_dict in start_end_intervals[1:]:
        driver = ingest_search_results(
            "Lockheed Martin",
            date_range_dict,
            driver,
            sqlite_engine
        ) 