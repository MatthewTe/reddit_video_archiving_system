package com.reddit.label.GraphIngestor;

import static org.neo4j.driver.Values.parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.reddit.label.Databases.SubredditPost;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class SubredditPostGraphIngestor {

    public static RedditPostGraphIngestionResponse IngestSubredditPostVideo(SubredditPost post, MinioClient minioClient,  Driver neo4jDriver) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {
        /*
         *  * Migrates subreddit posts from a PostgreSQL database to a Neo4j graph database.
         * Downloads the post data from Minio, extracts relevant information, and creates nodes in Neo4j.
         *
         * @param post The subreddit post to be ingested.
         * @param minioClient The Minio client used to download the JSON post data.
         * @param neo4jDriver The Neo4j driver used to interact with the Neo4j graph database.
         * @return RedditPostGraphIngestionResponse containing the ingested Reddit post ID, video node filepath, 
         *         screenshot node path, and JSON node path, or null if the ingestion fails.
         */
        StringBuilder content = new StringBuilder();
        // Download and Parse the JSON object to get the title and the other parameters:
        try (InputStream stream = minioClient.getObject(
            GetObjectArgs.builder()
            .bucket("reddit-posts")
            .object(post.getJsonPostPath())
            .build())) {

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
            }
        }

        System.out.println("Beginning parsing JSON object to extract title and created date");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(content.toString());
        
        String redditPostTitle = null;
        String redditPostCreataedDate = null;

        if (rootNode.isArray()) {
            ArrayNode rootArrayNode = (ArrayNode)rootNode;
            JsonNode firstArrayItem = rootArrayNode.get(0);
            JsonNode mainAttributesNode = firstArrayItem.get("data").get("children").get(0).get("data");

            redditPostTitle = mainAttributesNode.get("title").asText();
            redditPostCreataedDate = mainAttributesNode.get("created").asText();
            
            System.out.printf("Extracted title for reddit post: %s \n", redditPostTitle);
            System.out.printf("Extracted created date %s for reddit post: %s \n", redditPostCreataedDate, redditPostTitle);

        }            

        if ((redditPostTitle == null) || (redditPostCreataedDate == null)) {
            System.out.printf("Either post title or created date were null. Exiting ingestion for Reddit Post %s \n", post.getId());
            System.out.printf("redditPostTitle: %s \n", redditPostTitle);
            System.out.printf("redditPostCreataedDate: %s \n", redditPostCreataedDate);

            return null;
        }

        // Creating the necessary nodes in neo4j:
        try (Session session = neo4jDriver.session()) {
            
            @SuppressWarnings("unused")
            var createConstantResult = session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (initial_reddit_post:RedditPost) REQUIRE initial_reddit_post.id IS UNIQUE");
            System.out.println("Created unique constraint for reddit post nodes");

            var createQuery = new Query("""
                CREATE (inital_reddit_post:Entity:RedditPost {
                    title: $subreddit_title,
                    created: $date_created,
                    id: $id,
                    subreddit: $subreddit,
                    url: $url,
                    static_file_type: $static_file_type,
                    json: $json_filepath,
                    screenshot: $screenshot_static_path,
                    avg_location: null
                })

                CREATE (reddit_post_video:StaticFile:Reddit:Video {
                    filepath: $filestream_path,
                    avg_location: null
                })

                CREATE (inital_reddit_post)-[:CONTAINED]->(reddit_post_video)
                CREATE (reddit_post_video)-[:EXTRACTED_FROM]->(inital_reddit_post)

                RETURN inital_reddit_post.id, inital_reddit_post.json, inital_reddit_post.screenshot, reddit_post_video.filepath;
                """,
                parameters(
                    "id", post.getId(),
                    "subreddit_title", redditPostTitle,
                    "date_created", redditPostCreataedDate,
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
                String screenshotNode = record.get("inital_reddit_post.screenshot").asString();
                String jsonNode = record.get("inital_reddit_post.json").asString();

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
