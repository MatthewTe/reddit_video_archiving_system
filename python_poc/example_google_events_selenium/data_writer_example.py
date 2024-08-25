from data_writers.drivers.sqlite_driver import insert_sqlite_search_query, insert_sqlite_search_query_result, create_sqlite_search_result_tables
from data_writers.ingestors.chromium_ingestors import ingest_search_results
from testing_data.load_testing_data import load_full_seach_page

from selenium import webdriver
import sqlalchemy as sa
import pandas as pd
from datetime import datetime
import sys
from lxml import etree

if __name__ == '__main__':

    sqlite_engine = sa.create_engine("sqlite:///testing_run.sqlite")
    
    create_sqlite_search_result_tables(sqlite_engine)   

    driver = webdriver.Chrome()

    ingest_search_results(
        search_query='"North Korea"',
        driver=driver,
        db_engine=sqlite_engine,
        after="2020-06-03",
        before="2020-06-08",
        insert_search_query_func=insert_sqlite_search_query,
        insert_search_result_func=insert_sqlite_search_query_result
    )
    
    '''

    ingest_search_results(
            search_query='frontier markets equity research',
            driver=driver,
            db_engine=sqlite_engine,
            filetype="pdf",
            insert_search_query_func=insert_sqlite_search_query,
            insert_search_result_func=insert_sqlite_search_query_result
        )
    
    '''