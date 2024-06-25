package com.reddit.label.SubredditIngestor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.reddit.label.BlobStorage.BlobStorageConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;

import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class SubredditStaticContentIngestorTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DB.connectTestDB();

        SubredditTablesDB.createSubredditTables(conn);

        try (PreparedStatement pstmt = conn.prepareStatement("TRUNCATE TABLE subreddit_posts")) {
            pstmt.executeUpdate();
        }

        conn.setAutoCommit(false);
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.rollback();
        conn.close();
    }

    @Test
    void testIngestJSONContent() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException {
        SubredditPost exampleJsonSubredditPost = new SubredditPost(
            "example_json_id",
            "ExampleSubreddit",
            "https://www.reddit.com/r/pics/comments/1dgpdng/the_british_aircraft_carrier_hms_queen_elizabeth/",
            false,
            null,
            null,
            null,
            null,
            null
        );

        List<SubredditPost> examplePosts = new ArrayList<SubredditPost>();
        examplePosts.add(exampleJsonSubredditPost);

        int rowsInserted = SubredditTablesDB.InsertFullSubredditPost(conn, examplePosts);
        assertEquals(1, rowsInserted);

        SubredditPost postFromDB = SubredditTablesDB.getPost(conn, "example_json_id");
        assertEquals("example_json_id", postFromDB.getId());
        
        MinioClient testClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint(), 9000, false)
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();

        String staticFileJsonPath = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, postFromDB);
        System.out.println(staticFileJsonPath);
        assertEquals("example_json_id/post.json", staticFileJsonPath);

        SubredditPost postFromDatabase = SubredditTablesDB.getPost(conn, "example_json_id");
        assertEquals("example_json_id/post.json", postFromDatabase.getJsonPostPath());

        RemoveObjectArgs rArgs = RemoveObjectArgs.builder()
            .bucket("reddit-posts")
            .object(staticFileJsonPath)
            .build();

        testClient.removeObject(rArgs);

    }

    @Test
    void testIngestSnapshotImage() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {
        SubredditPost exampleScreenshotSubredditPost = new SubredditPost(
            "example_screenshot_id",
            "ExampleSubreddit",
            "https://www.reddit.com/r/pics/comments/1dgpdng/the_british_aircraft_carrier_hms_queen_elizabeth/",
            false,
            null,
            null,
            null,
            null,
            null
        );

        List<SubredditPost> examplePosts = new ArrayList<SubredditPost>();
        examplePosts.add(exampleScreenshotSubredditPost);

        int rowsInserted = SubredditTablesDB.InsertFullSubredditPost(conn, examplePosts);
        assertEquals(1, rowsInserted);

        SubredditPost postFromDB = SubredditTablesDB.getPost(conn, "example_screenshot_id");
        assertEquals("example_screenshot_id", postFromDB.getId());
        
        MinioClient testClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint(), 9000, false)
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();

        String staticFileScreenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, testClient, postFromDB);
        System.out.println(staticFileScreenshotPath);
        assertEquals("example_screenshot_id/screenshot.png", staticFileScreenshotPath);

        SubredditPost postFromDatabase = SubredditTablesDB.getPost(conn, "example_screenshot_id");
        assertEquals("example_screenshot_id/screenshot.png", postFromDatabase.getScreenshotPath());

        RemoveObjectArgs rArgs = RemoveObjectArgs.builder()
            .bucket("reddit-posts")
            .object(staticFileScreenshotPath)
            .build();

        testClient.removeObject(rArgs);

    }
    
}
