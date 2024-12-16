package com.reddit.label.GraphIngestor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.minio.connections.MinioHttpConnector;
import com.reddit.label.minio.environments.MinioTestEnvironmentProperties;
import com.reddit.label.neo4j.connections.Neo4jConnector;
import com.reddit.label.neo4j.environments.Neo4jTestEnvironmentProperties;
import com.reddit.label.postgres.connections.PostgresConnector;
import com.reddit.label.postgres.environments.PostgresTestEnvironmentProperties;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class SubredditPostGraphIngestorTest {

    private Driver driver;
    private MinioClient minioClient;
    private Connection conn;
    private InputStream noPostJsonStream;

    @BeforeEach
    void setUp() throws SQLException, IOException {

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        driver = neo4jConnector.getDriver();

        MinioTestEnvironmentProperties minioEnvironent = new MinioTestEnvironmentProperties();
        minioEnvironent.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");       
        MinioHttpConnector minioConnector = new MinioHttpConnector();
        minioConnector.loadEnvironment(minioEnvironent);

        minioClient = minioConnector.getClient();        

        PostgresTestEnvironmentProperties psqlEnvironment = new PostgresTestEnvironmentProperties();
        psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        PostgresConnector psqlConnector = new PostgresConnector();
        psqlConnector.loadEnvironment(psqlEnvironment);
        
        conn = psqlConnector.getConnection();

        SubredditTablesDB.createSubredditTables(conn);

        try (PreparedStatement pstmt = conn.prepareStatement("TRUNCATE TABLE subreddit_posts")) {
            pstmt.executeUpdate();
        }

        conn.setAutoCommit(false);

        noPostJsonStream = getClass().getClassLoader().getResourceAsStream("test-data/no_post_hint.json");
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
    void testIngestSubredditPostVideo() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {

        StringBuilder jsonStringBuilder = new StringBuilder();
        try (BufferedReader jsonReader = new BufferedReader(new InputStreamReader(noPostJsonStream))) {
            String line;
            while ((line = jsonReader.readLine()) != null) {
                jsonStringBuilder.append(line);
                jsonStringBuilder.append(System.lineSeparator());
            } 

        byte[] jsonByteContent = jsonStringBuilder.toString().getBytes();

        ObjectWriteResponse jsonUploadResponse = minioClient.putObject(
            PutObjectArgs.builder().bucket("reddit-posts").object("example_json_file_post/json_id.json")
                .stream(new ByteArrayInputStream(jsonByteContent), jsonByteContent.length, -1)
                .contentType("application/json")
                .build()
        );

        System.out.printf("Inserted Json file: $s", jsonUploadResponse.toString());

        SubredditPost examplePostToIngest = new SubredditPost(
            "example_id",
            "example_screenshot_path/screenshot.png", 
            "example_json_file_post/json_id.json", 
           "example_screenshot_path/screenshot.png" 
        );

        RedditPostGraphIngestionResponse postIngestionResponse = SubredditPostGraphIngestor.IngestSubredditPostVideo(examplePostToIngest, minioClient, driver);
        
        assertEquals("example_id", postIngestionResponse.getRedditPostId());
        assertEquals("example_screenshot_path/screenshot.png", postIngestionResponse.getScreenshotFileName());
        assertEquals("example_json_file_post/json_id.json", postIngestionResponse.getJsonFileName());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
