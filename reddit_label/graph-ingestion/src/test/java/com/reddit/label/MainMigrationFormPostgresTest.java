package com.reddit.label;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import com.reddit.label.BlobStorage.MinioClientConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.GraphIngestor.RedditPostGraphIngestionResponse;
import com.reddit.label.GraphIngestor.SubredditPostGraphIngestor;
import com.reddit.label.Parsers.RedditJsonParser;
import com.reddit.label.StaticFileIngestors.RedditHostedVideoIngestor;
import com.reddit.label.SubredditIngestor.SubredditStaticContentIngestor;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class MainMigrationFormPostgresTest {

    private Driver driver;
    private MinioClient minioClient;
    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {

        driver = DB.connectTestGraphB();
        minioClient = MinioClientConfig.geTestMinioClient();        
        conn = DB.connectTestDB();
        System.out.println(conn);

        SubredditTablesDB.createSubredditTables(conn);

        try (PreparedStatement pstmt = conn.prepareStatement("TRUNCATE TABLE subreddit_posts")) {
            pstmt.executeUpdate();
        }

        conn.setAutoCommit(false);
    }

    @AfterEach
    void tearDown() throws SQLException {

        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
            System.out.println("Clear the neo4j database");
        } finally {
            driver.close();
        }

        conn.rollback();
        conn.close();
    }

    @Test
    void testFullEndToEndPostgresToGraphMigration() throws InterruptedException, InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {

        // Insert several subreddit posts into the postgres database.
        // Ingest the static file content from the post into minio.
        // Check the graph database to make sure that there are no nodes in it.
        // Run the migration - check to see which nodes were inserted into the graph db.
        // Run it again to confirm that it doesn't re-import existing features.
        SubredditPost post1 = new SubredditPost(
            "example_graph_migration_one",
            "CombatFootage",
            "https://www.reddit.com/r/CombatFootage/comments/1doe86m/a_tank_of_the_92nd_assault_brigade_of_the/",
            false,
            null,
            null,
            null,
            null,
            null
        );
        SubredditPost post2 = new SubredditPost(
            "example_graph_migration_two",
            "CombatFootage",
            "https://www.reddit.com/r/CombatFootage/comments/1e98jrg/ukrainian_drone_attack_on_the_tuapse_refinery_in/",
            false,
            null,
            null,
            null,
            null,
            null
        );
        List<SubredditPost> postsToIngest = new ArrayList<>(List.of(post1, post2));

        int rowsInserted = SubredditTablesDB.InsertFullSubredditPost(conn, postsToIngest);
        assertEquals(2, rowsInserted);
        
        for (SubredditPost post: postsToIngest) {
                
            Thread.sleep(4000);

            System.out.println(post.getId());

            String jsonFileName;
            if (post.getJsonPostPath() == null) {
                System.out.printf("%s post has no json file. Downloading json file. \n", post.getId());
                jsonFileName = SubredditStaticContentIngestor.IngestJSONContent(conn, minioClient, post);
                System.out.printf("json file for %s post has been ingested as %s \n", post.getId(), jsonFileName);
            } else {
                jsonFileName = post.getJsonPostPath();
            }

            if (post.getScreenshotPath() == null) {
                System.out.printf("%s post has no screenshot. Taking screenshot\n", post.getId());
                String screenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, minioClient, post);
                System.out.printf("Screenshot for %s post has been ingessted at %s \n", post.getId(), screenshotPath);
            }


            System.out.printf("Getting json post from database from post %s \n", post.getId());
            try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                .bucket("reddit-posts")
                .object(jsonFileName)
                .build())) {

                // Reading the input Stream:
                StringBuilder content = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                    String line;
                    while((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }
                }

                System.out.printf("Beginning Json Parsing \n");

                Thread.sleep(500);
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(content.toString());
                
                if (parsedJsonResponse.getInitalPostFields().getStaticFileType() == RedditContentPostType.HOSTED_VIDEO) {

                    System.out.printf("Post %s has a static file type of video:hosted. Beginning static file ingestion.", post.getId());

                    Thread.sleep(500);

                    RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                    hostedVideoIngestor.fileToBlob(
                        post, 
                        parsedJsonResponse.getInitalPostFields(),
                        parsedJsonResponse.getFullPostJson(),
                        conn,
                        minioClient);

                        System.out.printf("Finished ingesting all data for post %s \n", post.getId());

                    Thread.sleep(500);

                } else {
                    // Update here as I add parsers:
                    System.out.printf(
                        "Parsed JSON from post %s. The static file type was %s which is not currently supported. Setting static_file_type to unknown", 
                        post.getId(),
                        parsedJsonResponse.initalPostFields.getStaticFileType()
                    );

                    int updatedStaticFileTypeResponse = SubredditTablesDB.updateStaticFileType(conn, post.getId(), "unknown");
                    if (updatedStaticFileTypeResponse != 1) {
                        System.out.printf("Error in updating static file type. Result integer returned is: %d \n", updatedStaticFileTypeResponse);
                    } else {
                        System.out.printf("Sucessfully set the static file type for post %s to unknown", post.getId());
                    }
                }

                List<SubredditPost> staticPostsToIngest = SubredditTablesDB.getPostsBasedOnStaticFileType(conn, "hosted:video");
    
                // Getting list of Ids already in Graph database:
                List<String> existingGraphPosts = new ArrayList<>();
                var result = driver.executableQuery("MATCH (n:Entity:RedditPost) return n.id").execute();
                var records = result.records();
                records.forEach(r -> {
                    existingGraphPosts.add(r.get("id").asString());
                });
                
                List<SubredditPost> newPostsToIngest = staticPostsToIngest.stream().filter(newPost -> !existingGraphPosts.contains(newPost.getId())).collect(Collectors.toList());

                // Iterate through each post and ingests the item into the Neo4J database: 
                for (SubredditPost newPost: newPostsToIngest) {

                    System.out.printf("Beginning to insert reddit post %s to graph database\n", newPost.getId());
                    try {
                        RedditPostGraphIngestionResponse postIngestionResponse = SubredditPostGraphIngestor.IngestSubredditPostVideo(newPost, minioClient, driver);
                        System.out.printf("Ingested the subreddit post %s into the graph database:  \n", postIngestionResponse.getRedditPostId());

                    } catch (Exception e) {
                        System.out.printf("Error in ingesting reddit post %s \n", newPost.getId());
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
