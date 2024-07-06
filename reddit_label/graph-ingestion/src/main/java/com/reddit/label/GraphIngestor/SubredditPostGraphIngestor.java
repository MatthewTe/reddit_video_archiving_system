package com.reddit.label.GraphIngestor;

import static org.neo4j.driver.Values.parameters;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import com.reddit.label.Databases.SubredditPost;

public class SubredditPostGraphIngestor {

    public static RedditPostGraphIngestionResponse IngestSubredditPostVideo(SubredditPost post, Driver neo4jDriver) {

        // Creating the necessary nodes in neo4j:
        try (Session session = neo4jDriver.session()) {
            
            @SuppressWarnings("unused")
            var createConstantResult = session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (initial_reddit_post:RedditPost) REQUIRE initial_reddit_post.id IS UNIQUE");
            System.out.println("Created unique constraint for reddit post nodes");

            var createQuery = new Query("""
                CREATE (inital_reddit_post:Entity:RedditPost {
                    id: $id,
                    subreddit: $subreddit,
                    url: $url,
                    static_file_type: $static_file_type,
                    avg_location: null
                })

                CREATE (reddit_post_video:StaticFile:Reddit:Video {
                    filepath: $filestream_path,
                    avg_location: null
                })

                CREATE (reddit_post_screenshot:StaticFile:RedditPost:Screenshot {
                    filepath: $screenshot_static_path,
                    avg_location: null
                })

                CREATE (reddit_post_json:StaticFile:RedditPost:Json {
                    filepath: $json_filepath,
                    avg_location: null
                })

                CREATE (inital_reddit_post)-[:CONTAINED]->(reddit_post_video)
                CREATE (reddit_post_video)-[:EXTRACTED_FROM]->(inital_reddit_post)

                CREATE (inital_reddit_post)-[:CONTAINED]->(reddit_post_screenshot)
                CREATE (reddit_post_screenshot)-[:EXTRACTED_FROM]->(inital_reddit_post)

                CREATE (inital_reddit_post)-[:CONTAINED]->(reddit_post_json)
                CREATE (reddit_post_json)-[:EXTRACTED_FROM]->(inital_reddit_post)

                RETURN inital_reddit_post.id, reddit_post_video.filepath, reddit_post_screenshot.filepath, reddit_post_json.filepath;
                """,
                parameters(
                    "id", post.getId(), 
                    "subreddit", post.getSubreddit(),
                    "url", post.getUrl(),
                    "static_file_type", post.getStaticFileType(),
                    "filestream_path", String.format("%shosted_video.mpd", post.getStaticRootPath()),
                    "screenshot_static_path", post.getScreenshotPath(),
                    "json_filepath",post.getJsonPostPath()
                    )
                );

            Result result = session.run(createQuery);
            
            while (result.hasNext()) {
                var record = result.next();
                String redditPost = record.get("inital_reddit_post.id").asString();
                String videoNode = record.get("reddit_post_video.filepath").asString();
                String screenshotNode = record.get("reddit_post_screenshot.filepath").asString();
                String jsonNode = record.get("reddit_post_json.filepath").asString();

                System.out.printf("Ingested Reddit Post: %s \n", redditPost);
                System.out.printf("Ingested Video Node: %s \n", videoNode);
                System.out.printf("Ingested Screenshot Node: %s \n", screenshotNode);
                System.out.printf("Ingested Json Node: %s \n",jsonNode);
                
                RedditPostGraphIngestionResponse ingestionResponse = new RedditPostGraphIngestionResponse(
                    redditPost, videoNode, screenshotNode, jsonNode);
                
                return ingestionResponse;
            }

        }

        return null;
    }

}
