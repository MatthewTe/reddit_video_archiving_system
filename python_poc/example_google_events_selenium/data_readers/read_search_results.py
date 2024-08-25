import sqlalchemy as sa
import pandas as pd

def get_search_results(search_query: str) -> pd.DataFrame:

    sqlite_engine = sa.create_engine("sqlite:///testing_run.sqlite")

    with sqlite_engine.connect() as conn, conn.begin():

        df = pd.read_sql(sa.text(
            """SELECT 
                results.id AS search_result_id, 
                LUT.search_query_id AS basic_query_id,
                LUT.full_query_id AS full_query_id,
                results.title AS title, 
                results.published_date AS published_date,
                query.full_query AS full_query_text,
                query.runtime AS query_runtime,
                results.result_html AS result_html,
                results.modal AS modal_html
            FROM search_results AS results
            JOIN search_results_LUT AS LUT
            ON LUT.search_result_id = results.id
            JOIN search_queries as query
            ON LUT.search_query_id = query.id
            WHERE query.query = :search_query;
            """),
            con=conn,
            params={'search_query': search_query}
        )

    return df