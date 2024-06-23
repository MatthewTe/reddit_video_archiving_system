package com.reddit.label.StaticFileIngestors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.reddit.label.RedditContentPostType;
import com.reddit.label.RedditJsonParserResponse;
import com.reddit.label.RedditPostJsonDefaultAttributes;
import com.reddit.label.BlobStorage.BlobStorageConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.Parsers.RedditJsonParser;
import com.reddit.label.SubredditIngestor.SubredditStaticContentIngestor;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;

public class RedditHostedVideoIngestorTest {

    Connection conn;

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
    void testFileToBlobHostedVideoTypeEndToEnd() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, JsonProcessingException, IOException {

        // First ingest a reddit post that has a hosted:video url.
        // Download the json content to Minio.
        // Run the static file ingestor to download the static file.
        // Validate that the statc files have been ingested and the static boolean field in the database has changed.
        
        // String id, String subreddit, String url, boolean staticDownloaded
        SubredditPost examplePostWHostedVideo = new SubredditPost(
            "example_hosted_video_id",
            "CombatFootage",
            "https://www.reddit.com/r/CombatFootage/comments/1dlxcew/p40_fighter_bomber_set_alight_by_light_flak_over/",
            false
        );

        List<SubredditPost> postToInsert = new ArrayList<>();
        postToInsert.add(examplePostWHostedVideo);
        int insertedSubredditPostResult = SubredditTablesDB.InsertBasicSubredditPost(conn, postToInsert);
        assertEquals(insertedSubredditPostResult, 1);

        // Download the JSON:
        MinioClient testClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint())
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();

        String jsonPath = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, examplePostWHostedVideo);
        assertEquals("example_hosted_video_id/post.json", jsonPath);

        try (InputStream jsonStream = testClient.getObject(
            GetObjectArgs.builder()
            .bucket("reddit-posts")
            .object(jsonPath)
            .build()
        )) {

            StringBuilder jsonStringBuilder = new StringBuilder();
            try (BufferedReader jsonReader = new BufferedReader(new InputStreamReader(jsonStream))) {
                String line;
                while ((line = jsonReader.readLine()) != null) {
                    jsonStringBuilder.append(line);
                    jsonStringBuilder.append(System.lineSeparator());
                }
                
                // Downloading the static file:
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(jsonStringBuilder.toString());
                RedditPostJsonDefaultAttributes jsonAttributes = parsedJsonResponse.getInitalPostFields();

                assertEquals(jsonAttributes.getId(), "1dlxcew");
                assertEquals(jsonAttributes.getStaticFileType(), RedditContentPostType.HOSTED_VIDEO);
                
                // Running static file ingestion logic:
                RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                hostedVideoIngestor.fileToBlob(
                    examplePostWHostedVideo, 
                    jsonAttributes, 
                    parsedJsonResponse.getFullPostJson(), 
                    conn, 
                    testClient
                );

                // QA checking the outputs:
                /* 
                 * The following objects should be in minio blob storage:
                 *  - mpd file - hosted_video.mpd
                 *  - json file post.json
                 *  - DASH mp4 video file - DASH_720.mp4
                 *  - DASH mp4 audio file - DASH_AUDIO_128.mp4
                */
                Iterable<Result<Item>> staticPostResults = testClient.listObjects(
                    ListObjectsArgs.builder()
                        .bucket("reddit-posts")
                        .prefix("example_hosted_video_id/")
                        .build()
                );

                List<String> allFilenamesFromBlob = new ArrayList<>();
                for (Result<Item> result: staticPostResults) {

                    Item item = result.get();
                    allFilenamesFromBlob.add(item.objectName());
                    System.out.println(item.objectName());

                }

               List<String> expectedFilenames = new ArrayList<>();
                expectedFilenames.add("example_hosted_video_id/post.json");
                expectedFilenames.add("example_hosted_video_id/hosted_video.mpd");
                expectedFilenames.add("example_hosted_video_id/DASH_720.mp4");
                expectedFilenames.add("example_hosted_video_id/DASH_AUDIO_64.mp4");

                // Checking to see if all of the files are in blob:
                for (String expectedFilename: expectedFilenames) {
                    assertTrue(allFilenamesFromBlob.contains(expectedFilename));
                }

                // Checking the postgres database to see if the static ingested flag has been set to true:
                SubredditPost updatedPost = SubredditTablesDB.getPost(conn, "example_hosted_video_id");

                assertEquals(updatedPost.getId(), "example_hosted_video_id");
                assertTrue(updatedPost.isStaticDownloaded());

            }

        }
        
    }

    @Test
    void testFileToBlobAllOtherTypes() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {
        // Should run and create the reddit post in the database and minio but should not affect the state of the system because it is the wrong content type for the video ingestor:

        // First ingest a reddit post that has a any other static file type.
        // Download the json content to Minio.
        // Run the static file ingestor to download the static file.
        // Validate that no statc files have downloaded and that the state of the application hasn't been changed
        // String id, String subreddit, String url, boolean staticDownloaded
        SubredditPost examplePostWPicture = new SubredditPost(
            "example_hosted_pic_id",
            "CombatFootage",
            "https://www.reddit.com/r/pics/comments/1dgpdng/the_british_aircraft_carrier_hms_queen_elizabeth/",
            false
        );

        List<SubredditPost> postToInsert = new ArrayList<>();
        postToInsert.add(examplePostWPicture);
        int insertedSubredditPostResult = SubredditTablesDB.InsertBasicSubredditPost(conn, postToInsert);
        assertEquals(insertedSubredditPostResult, 1);

        // Download the JSON:
        MinioClient testClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint())
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();

        String jsonPath = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, examplePostWPicture);
        assertEquals("example_hosted_pic_id/post.json", jsonPath);

        try (InputStream jsonStream = testClient.getObject(
            GetObjectArgs.builder()
            .bucket("reddit-posts")
            .object(jsonPath)
            .build()
        )) {

            StringBuilder jsonStringBuilder = new StringBuilder();
            try (BufferedReader jsonReader = new BufferedReader(new InputStreamReader(jsonStream))) {
                String line;
                while ((line = jsonReader.readLine()) != null) {
                    jsonStringBuilder.append(line);
                    jsonStringBuilder.append(System.lineSeparator());
                }
                
                // Downloading the static file:
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(jsonStringBuilder.toString());
                RedditPostJsonDefaultAttributes jsonAttributes = parsedJsonResponse.getInitalPostFields();

                assertEquals(jsonAttributes.getId(), "1dgpdng");
                assertNotEquals(jsonAttributes.getStaticFileType(), RedditContentPostType.HOSTED_VIDEO);
                
                // Running static file ingestion logic:
                RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                String videoIngestionResponse = hostedVideoIngestor.fileToBlob(
                    examplePostWPicture, 
                    jsonAttributes, 
                    parsedJsonResponse.getFullPostJson(), 
                    conn, 
                    testClient
                );
                assertNull(videoIngestionResponse);

                // QA checking the outputs:
                /* 
                 * The following objects should be in minio blob storage:
                 *  - json file post.json
                 * 
                 *  Nothing else
                */
                Iterable<Result<Item>> staticPostResults = testClient.listObjects(
                    ListObjectsArgs.builder()
                        .bucket("reddit-posts")
                        .prefix("example_hosted_pic_id/")
                        .build()
                );

                List<String> allFilenamesFromBlob = new ArrayList<>();
                for (Result<Item> result: staticPostResults) {

                    Item item = result.get();
                    allFilenamesFromBlob.add(item.objectName());
                    System.out.println(item.objectName());

                }

               List<String> expectedFilenames = new ArrayList<>();
                expectedFilenames.add("example_hosted_pic_id/post.json");

                List<String> unexpectedFilenames = new ArrayList<>();
                unexpectedFilenames.add("example_hosted_pic_id/hosted_video.mpd");
                unexpectedFilenames.add("example_hosted_pic_id/DASH_720.mp4");
                unexpectedFilenames.add("example_hosted_pic_id/DASH_AUDIO_64.mp4");

                // Checking to see if all of the files are in blob:
                for (String expectedFilename: expectedFilenames) {
                    assertTrue(allFilenamesFromBlob.contains(expectedFilename));
                }

                for (String unexpectedFilename: unexpectedFilenames) {
                    assertFalse(allFilenamesFromBlob.contains(unexpectedFilename));
                }

                // Checking the postgres database to see if the static ingested flag has been set to true:
                SubredditPost updatedPost = SubredditTablesDB.getPost(conn, "example_hosted_pic_id");

                assertEquals(updatedPost.getId(), "example_hosted_pic_id");
                assertFalse(updatedPost.isStaticDownloaded());

            }

        }
       
    }
}
