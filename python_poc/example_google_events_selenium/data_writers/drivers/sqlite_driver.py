from data_writers.custom_types.search_results_types import SearchQuery, SearchQueryResult

import sqlalchemy as sa
import pandas as pd
from loguru import logger

def create_sqlite_search_result_tables(engine: sa.engine.Engine):
    with engine.connect() as conn, conn.begin():
        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_queries (
                id TEXT,
                query TEXT,
                full_query TEXT,
                full_query_id TEXT,
                start_date TEXT,
                end_date TEXT,
                site TEXT,
                filetype TEXT,
                runtime TEXT
            )
            """
        ))

        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_results_LUT (
                search_query_id TEXT,
                search_result_id TEXT,
                full_query_id TEXT
            )
            """
        ))

        conn.execute(sa.text(
            """CREATE TABLE IF NOT EXISTS search_results (
                id TEXT,
                title TEXT,
                url TEXT,
                published_date TEXT,
                result_html TEXT,
                modal TEXT
            )
            """
        ))

def insert_sqlite_search_query(engine: sa.engine.Engine, search_query_db_params: SearchQuery):

    with engine.connect() as conn, conn.begin():

        check_for_duplicate_query = conn.execute(sa.text(
            """SELECT id FROM search_queries WHERE full_query_id = :full_query_id
            """
            ),
            {
                "full_query_id": search_query_db_params['full_query_id']
            }
        )

        if check_for_duplicate_query.rowcount > 1:
            logger.warning(f"{search_query_db_params['full_query_id']} already exists in the database. Skipping")
        else:
            result = conn.execute(
                sa.text("""
                    INSERT INTO search_queries (
                        id, 
                        query, 
                        full_query, 
                        full_query_id, 
                        start_date, 
                        end_date, 
                        site,
                        filetype,
                        runtime
                    ) 
                    VALUES (:id, :query, :full_query, :full_query_id, :start_date, :end_date, :site, :filetype, :runtime)"""
                ),
                search_query_db_params
            )
        
            if result.rowcount > 0:
                logger.debug("Inserted record of search query into the database")
            else:
                raise ValueError(f"Rowcount for insert query into search_query table was 0. Did not insert record of query into db {search_query_db_params['full_query_id']}")

def insert_sqlite_search_query_result(engine: sa.engine.Engine, search_query_results_db_params: SearchQueryResult):

    with engine.connect() as conn, conn.begin():
        
        search_result_LUT_df = pd.read_sql(
            sa.text("SELECT * FROM search_results_LUT WHERE search_result_id = :search_result_id"),
            con=conn,
            params={'search_result_id': search_query_results_db_params['id']}
        )

        # A completely new record never before seen:
        if search_result_LUT_df.empty:
            logger.info(f"There are no entires for search_result {search_query_results_db_params['id']} in the LUT. Creating entries in LUT and search_results table")

            # Inserting record of search result into the search result table:
            search_result_insertion = conn.execute(sa.text(
                """INSERT INTO search_results (id, title, url, published_date, result_html, modal)
                VALUES (:id, :title, :url, :published_date, :result_html, :modal)
                """),
                {
                    'id':search_query_results_db_params['id'],
                    'title': search_query_results_db_params['title'],
                    'url': search_query_results_db_params['url'],
                    'result_html': search_query_results_db_params['result_html'],
                    'published_date': search_query_results_db_params['published_date'],
                    'modal': search_query_results_db_params['modal'],
                }
            )

            if search_result_insertion.rowcount == 1:
                logger.info(f"\n Sucessfully inserted result {search_query_results_db_params['id']} into search_result table. Trying new record into the LUT \n")
                LUT_insertion_result = conn.execute(sa.text(
                    """INSERT INTO search_results_LUT (search_query_id, search_result_id, full_query_id) VALUES (:search_query_id, :search_result_id, :full_query_id)
                    """),
                    {
                        'search_query_id': search_query_results_db_params['query_id'],
                        'search_result_id': search_query_results_db_params['id'],
                        'full_query_id': search_query_results_db_params['full_query_id']
                    }
                )

                if LUT_insertion_result.rowcount == 1:
                    logger.info(f"\nInserted search result into lookup in LUT {search_query_results_db_params['id']} - {search_query_results_db_params['query_id']}\n")
                else:
                    raise ValueError(f"There was an error in inserting a record into the LUT table. Rowcount was not 1 {LUT_insertion_result.rowcount} {search_query_results_db_params['id']} - {search_query_results_db_params['query_id']}")

            else:
                raise ValueError(f"There was an error in inserting a record into the search_reslts table. The result rowcount was not 1: {search_result_insertion.rowcount} \n {search_query_results_db_params}")

        # A record exists in the LUT:
        else:
            logger.info(f"There are {len(search_result_LUT_df)} entries in the lookup table for the search result: {search_query_results_db_params['id']}")

            # Checking if a query_id and search_result_id exists in the LUT:
            query_id_specific_LUT_df = search_result_LUT_df[search_result_LUT_df['full_query_id'] == search_query_results_db_params['full_query_id']]
            if not query_id_specific_LUT_df.empty:
                logger.warning(f"There already exists a record in the LUT that contains both query id: {search_query_results_db_params['full_query_id']} and result id: {search_query_results_db_params['id']} no need to insert any records.")

            else:
                logger.info(f"There are no records that exist in the LUT that contains both query id: {search_query_results_db_params['full_query_id']} and result id: {search_query_results_db_params['id']}")
                LUT_insertion_result = conn.execute(
                    sa.text("INSERT INTO search_results_LUT (search_query_id, search_result_id, full_query_id) VALUES (:search_query_id, :search_result_id, :full_query_id)"),
                    {
                        'search_query_id': search_query_results_db_params['query_id'],
                        'search_result_id': search_query_results_db_params['id'],
                        'full_query_id': search_query_results_db_params['full_query_id']
                    }
                )

                if LUT_insertion_result.rowcount == 1:
                    logger.info(f"Successfully inserted item into the LUT full_query_id: {search_query_results_db_params['full_query_id']} search_result_id: {search_query_results_db_params['id']}")
                else:
                    raise ValueError(f"There was some error in inserting a row into the LUT. Rowcount was not 1: {LUT_insertion_result.rowcount}  {search_query_results_db_params['id']} - {search_query_results_db_params['query_id']}")

