from neo4j import GraphDatabase
import uuid

if __name__ == "__main__":


    with GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test_password")) as driver:
        records, summary, keys = driver.execute_query(
            """
            OPTIONAL MATCH (n:Entity:Post:Reddit)
            WHERE n.id IN $existing_ids
            RETURN 
                n.id AS id,
                n IS NOT NULL AS exists, 
                COALESCE(labels(n), []) AS node_labels

            """,
            existing_ids=["fe101548-19bf-32a8-b55e-553c39e7d12f", "dcdccfb1-14f2-357b-9f68-f462dc302bf3"],
            database_="neo4j",
        )

        existing_posts = [post.get("id") for post in list(records) if post.get("exists")== True]
            
        post = {}
        
        records, summary, keys = driver.execute_query(
            """
            MERGE 
                (subreddit:Reddit:Subreddit:Entity 
                    {
                        id: $subreddit_id, 
                        subreddit_name: $subreddit_name
                    }
                ),
                (date:Date {id: $date_id, day: day}),
                (reddit_user:Reddit:User:Entity:Account 
                    {
                        id: $reddit_user_id,
                        author_name: $reddit_user_author_name,
                        author_full_name: $reddit_user_author_full_name
                    }
                )
            CREATE
                (reddit_screenshot_file:Reddit:Screenshot:StaticFile:Image 
                    {
                        id: $screenshot_id, 
                        path: $screenshot_path
                    }
                ),
                (reddit_json:Reddit:Json:StaticFile {id: $json_id, path: $json_path})
                (reddit_post:Entity:Post:Reddit 
                    {
                        id: $reddit_post_id,
                        url: $reddit_post_url,
                        title: $post_title,
                        static_root_url: $reddit_static_root_url,
                        static_downloaded: $reddit_static_downloaded_flag,
                        static_file_type: $reddit_static_file_type

                    }
                )
                (reddit_post)-[:POSTED_ON 
                    {
                        datetime: $reddit_post_created_date
                    }
                ]->(subreddit),
                (reddit_post)-[:POSTED_ON]->(date),
                (reddit_post)-[:TAKEN {datetime: $reddit_post_created_date}]->(reddit_screenshot_file),
                (reddit_post)-[:EXTRACTED {datetime: $reddit_post_created_date}]->(reddit_json),
                (reddit_user)-[:POSTED {datetime: $reddit_post_created_date}]->(reddit_post)
            """,
            subreddit_id=None,
            subreddit_name=None,
            date_id=None,
            day=None,
            reddit_user_id=None,
            reddit_user_author_name=None,
            reddit_user_author_full_name=None,
            screenshot_id=None,
            screenshot_path=None,
            json_id=None,
            json_path=None,
            reddit_post_id=None,
            reddit_post_url=None,
            post_title=None,
            reddit_static_root_url=None,
            reddit_static_downloaded_flag=None,
            reddit_static_file_type=None,
            reddit_post_created_date=None,
        )