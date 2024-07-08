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

import com.reddit.label.BlobStorage.BlobStorageConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;

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
    void setUp() throws SQLException {

        driver = DB.connectTestGraphB();
        minioClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint())
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();
        
            conn = DB.connectTestDB();

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
