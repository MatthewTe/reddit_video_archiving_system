package com.reddit.label;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.Parsers.RedditJsonParser;
import com.reddit.label.StaticFileIngestors.RedditHostedVideoIngestor;
import com.reddit.label.SubredditIngestor.SubredditStaticContentIngestor;
import com.reddit.label.minio.connections.MinioHttpConnector;
import com.reddit.label.minio.environments.MinioTestEnvironmentProperties;
import com.reddit.label.postgres.connections.PostgresConnector;
import com.reddit.label.postgres.environments.PostgresTestEnvironmentProperties;

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

public class EndToEndTests {

    Connection conn;

    @BeforeEach
    void setUp() throws Exception {

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

        /*
         * 
         * SubredditPost pictureUrlPost = new SubredditPost(
            "valid_picture_video_post",
            "NorthKoreaPics",
            "https://www.reddit.com/r/NorthKoreaPics/comments/e57y9n/a_north_korean_soldier_taking_in_the_city_view/", 
            false
        );
        postsForTesting.add(pictureUrlPost);
         * 
         */

    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.rollback();
        conn.close();
    }


    @Test
    void sucessfullStaticIngestionTestEndToEnd() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, JsonMappingException, JsonParseException, IOException {

        List<SubredditPost> postsForTesting = new ArrayList<>();
        SubredditPost hostedVideoUrlPost = new SubredditPost(
            "valid_hosted_video_post",
            "CombatFootage",
            "https://www.reddit.com/r/CombatFootage/comments/1doe86m/a_tank_of_the_92nd_assault_brigade_of_the/", 
            false
        );
        postsForTesting.add(hostedVideoUrlPost);
        int subredditIngestionResult = SubredditTablesDB.InsertBasicSubredditPost(conn, postsForTesting);
        assertEquals(1, subredditIngestionResult);

        // Getting a list of 1 subreddit posts to perform end-to-end ingestion:
        List<SubredditPost> postsToProcess = new ArrayList<>();
        SubredditPost correctVideoPost = SubredditTablesDB.getPost(conn, "valid_hosted_video_post");
        postsToProcess.add(correctVideoPost);

        MinioTestEnvironmentProperties minioEnvironent = new MinioTestEnvironmentProperties();
        minioEnvironent.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");       
        MinioHttpConnector minioConnector = new MinioHttpConnector();
        minioConnector.loadEnvironment(minioEnvironent);

        MinioClient testClient = minioConnector.getClient();

        for (SubredditPost post: postsToProcess) {
 
            System.out.println(post.getId());

            // Testing Json File Ingestion: 
            String jsonFileName;
            if (post.getJsonPostPath() == null) {
                System.out.printf("%s post has no json file. Downloading json file. \n", post.getId());
                jsonFileName = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, post);
                System.out.printf("json file for %s post has been ingested as %s \n", post.getId(), jsonFileName);
            } else {
                jsonFileName = post.getJsonPostPath();
            }

            // Test to see if json blob exists:
            Iterable<Result<Item>> staticPostResults = testClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket("reddit-posts")
                    .prefix("valid_hosted_video_post/")
                    .build()
            );

            List<String> allJsonFilenamesFromBlob = new ArrayList<>();
            for (Result<Item> result: staticPostResults) {

                Item item = result.get();
                allJsonFilenamesFromBlob.add(item.objectName());
                System.out.println(item.objectName());

            }
            SubredditPost postWUpdatedJsonFile = SubredditTablesDB.getPost(conn, "valid_hosted_video_post");

            assertEquals("valid_hosted_video_post/post.json", postWUpdatedJsonFile.getJsonPostPath());
            assertTrue(allJsonFilenamesFromBlob.contains("valid_hosted_video_post/post.json"));


            // Testing Screenshot PNG file ingestion:
            if (post.getScreenshotPath() == null) {
                System.out.printf("%s post has no screenshot. Taking screenshot\n", post.getId());
                String screenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, testClient, post);
                System.out.printf("Screenshot for %s post has been ingessted at %s \n", post.getId(), screenshotPath);
            }

            Iterable<Result<Item>> screenshotPostResults = testClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket("reddit-posts")
                    .prefix("valid_hosted_video_post/")
                    .build()
            );

            List<String> allScreenshotFilenamesFromBlob = new ArrayList<>();
            for (Result<Item> result: screenshotPostResults) {

                Item item = result.get();
                allScreenshotFilenamesFromBlob.add(item.objectName());
                System.out.println(item.objectName());
            }

            SubredditPost postWUpdatedScreenshotFile = SubredditTablesDB.getPost(conn, "valid_hosted_video_post");
            assertEquals("valid_hosted_video_post/screenshot.png", postWUpdatedScreenshotFile.getScreenshotPath());
            assertTrue(allScreenshotFilenamesFromBlob.contains("valid_hosted_video_post/screenshot.png"));

            
            // Begin parsing the Json file to extract static files:
            System.out.printf("Getting json post from database from post %s \n", post.getId());
            try (InputStream stream = testClient.getObject(
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
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(content.toString());

                assertEquals(parsedJsonResponse.getInitalPostFields().getStaticFileType(), RedditContentPostType.HOSTED_VIDEO);

                if (parsedJsonResponse.getInitalPostFields().getStaticFileType() == RedditContentPostType.HOSTED_VIDEO) {

                    System.out.printf("Post %s has a static file type of video:hosted. Beginning static file ingestion.", post.getId());

                    RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                    hostedVideoIngestor.fileToBlob(
                        post, 
                        parsedJsonResponse.getInitalPostFields(),
                        parsedJsonResponse.getFullPostJson(),
                        conn,
                        testClient);

                        System.out.printf("Finished ingesting all data for post %s \n", post.getId());

                        Iterable<Result<Item>> allStaticFile = testClient.listObjects(
                            ListObjectsArgs.builder()
                                .bucket("reddit-posts")
                                .prefix("valid_hosted_video_post/")
                                .build()
                        );

                        List<String> allBlobArrayFiles = new ArrayList<>();
                        for (Result<Item> result: allStaticFile) {

                            Item item = result.get();
                            allBlobArrayFiles.add(item.objectName());
                            System.out.println(item.objectName());

                        }

                        assertTrue(allBlobArrayFiles.contains("valid_hosted_video_post/DASH_720.mp4"));
                        assertTrue(allBlobArrayFiles.contains("valid_hosted_video_post/DASH_AUDIO_128.mp4"));
                        assertTrue(allBlobArrayFiles.contains("valid_hosted_video_post/hosted_video.mpd"));

                } else {
                    // Update here as I add parsers:
                    System.out.printf(
                        "Parsed JSON from post %s. The static file type was %s which is not currently supported. Setting static_file_type to unknown", 
                        parsedJsonResponse.initalPostFields.getStaticFileType()
                    );

                    int updatedStaticFileTypeResponse = SubredditTablesDB.updateStaticFileType(conn, post.getId(), "unknown");
                    if (updatedStaticFileTypeResponse != 1) {
                        System.out.printf("Error in updating static file type. Result integer returned is: %d \n", updatedStaticFileTypeResponse);
                    } else {
                        System.out.printf("Sucessfully set the static file type for post %s to unknown", post.getId());
                    }
                }
            }

            // QA-ing the subreddit post fields:
            SubredditPost fullyCompletePost = SubredditTablesDB.getPost(conn, "valid_hosted_video_post");
            assertEquals("valid_hosted_video_post", fullyCompletePost.getId());
            assertEquals("valid_hosted_video_post/post.json", fullyCompletePost.getJsonPostPath());
            assertEquals("valid_hosted_video_post/screenshot.png", fullyCompletePost.getScreenshotPath());
            assertEquals("hosted:video", fullyCompletePost.getStaticFileType());
            assertEquals("CombatFootage", fullyCompletePost.getSubreddit());
            assertEquals("valid_hosted_video_post/", fullyCompletePost.getStaticRootPath());
            assertEquals(true, fullyCompletePost.isStaticDownloaded());

        }
    }

    @Test
    void wrongTypeStaticIngestionTestEndToEnd() throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, JsonMappingException, JsonParseException, IOException {
        List<SubredditPost> postsForTesting = new ArrayList<>();
        SubredditPost pictureUrlPost = new SubredditPost(
            "invalid_hosted_video_post",
            "NorthKoreaPics",
            "https://www.reddit.com/r/NorthKoreaPics/comments/e57y9n/a_north_korean_soldier_taking_in_the_city_view/", 
            false
        );
        postsForTesting.add(pictureUrlPost);
        int subredditIngestionResult = SubredditTablesDB.InsertBasicSubredditPost(conn, postsForTesting);
        assertEquals(1, subredditIngestionResult);

        // Getting a list of 1 subreddit posts to perform end-to-end ingestion:
        List<SubredditPost> postsToProcess = new ArrayList<>();
        SubredditPost picturePost = SubredditTablesDB.getPost(conn, "invalid_hosted_video_post");
        postsToProcess.add(picturePost);

        MinioTestEnvironmentProperties minioEnvironent = new MinioTestEnvironmentProperties();
        minioEnvironent.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");       
        MinioHttpConnector minioConnector = new MinioHttpConnector();
        minioConnector.loadEnvironment(minioEnvironent);

        MinioClient testClient = minioConnector.getClient();

        for (SubredditPost post: postsToProcess) {
 
            System.out.println(post.getId());

            // Testing Json File Ingestion: 
            String jsonFileName;
            if (post.getJsonPostPath() == null) {
                System.out.printf("%s post has no json file. Downloading json file. \n", post.getId());
                jsonFileName = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, post);
                System.out.printf("json file for %s post has been ingested as %s \n", post.getId(), jsonFileName);
            } else {
                jsonFileName = post.getJsonPostPath();
            }

            // Test to see if json blob exists:
            Iterable<Result<Item>> staticPostResults = testClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket("reddit-posts")
                    .prefix("invalid_hosted_video_post/")
                    .build()
            );

            List<String> allJsonFilenamesFromBlob = new ArrayList<>();
            for (Result<Item> result: staticPostResults) {

                Item item = result.get();
                allJsonFilenamesFromBlob.add(item.objectName());
                System.out.println(item.objectName());

            }
            SubredditPost postWUpdatedJsonFile = SubredditTablesDB.getPost(conn, "invalid_hosted_video_post");

            assertEquals("invalid_hosted_video_post/post.json", postWUpdatedJsonFile.getJsonPostPath());
            assertTrue(allJsonFilenamesFromBlob.contains("invalid_hosted_video_post/post.json"));


            // Testing Screenshot PNG file ingestion:
            if (post.getScreenshotPath() == null) {
                System.out.printf("%s post has no screenshot. Taking screenshot\n", post.getId());
                String screenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, testClient, post);
                System.out.printf("Screenshot for %s post has been ingessted at %s \n", post.getId(), screenshotPath);
            }

            Iterable<Result<Item>> screenshotPostResults = testClient.listObjects(
                ListObjectsArgs.builder()
                    .bucket("reddit-posts")
                    .prefix("invalid_hosted_video_post/")
                    .build()
            );

            List<String> allScreenshotFilenamesFromBlob = new ArrayList<>();
            for (Result<Item> result: screenshotPostResults) {

                Item item = result.get();
                allScreenshotFilenamesFromBlob.add(item.objectName());
                System.out.println(item.objectName());
            }

            SubredditPost postWUpdatedScreenshotFile = SubredditTablesDB.getPost(conn, "invalid_hosted_video_post");
            assertEquals("invalid_hosted_video_post/screenshot.png", postWUpdatedScreenshotFile.getScreenshotPath());
            assertTrue(allScreenshotFilenamesFromBlob.contains("invalid_hosted_video_post/screenshot.png"));

            
            // Begin parsing the Json file to extract static files:
            System.out.printf("Getting json post from database from post %s \n", post.getId());
            try (InputStream stream = testClient.getObject(
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
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(content.toString());

                assertNotEquals(parsedJsonResponse.getInitalPostFields().getStaticFileType(), RedditContentPostType.HOSTED_VIDEO);

                if (parsedJsonResponse.getInitalPostFields().getStaticFileType() == RedditContentPostType.HOSTED_VIDEO) {

                    // Manually throwing because this logic branch should not get triggered:
                    assertEquals(true, false);

                    System.out.printf("Post %s has a static file type of video:hosted. Beginning static file ingestion.", post.getId());

                    RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                    hostedVideoIngestor.fileToBlob(
                        post, 
                        parsedJsonResponse.getInitalPostFields(),
                        parsedJsonResponse.getFullPostJson(),
                        conn,
                        testClient);

                        System.out.printf("Finished ingesting all data for post %s \n", post.getId());

                        Iterable<Result<Item>> allStaticFile = testClient.listObjects(
                            ListObjectsArgs.builder()
                                .bucket("reddit-posts")
                                .prefix("invalid_hosted_video_post/")
                                .build()
                        );

                        List<String> allBlobArrayFiles = new ArrayList<>();
                        for (Result<Item> result: allStaticFile) {

                            Item item = result.get();
                            allBlobArrayFiles.add(item.objectName());
                            System.out.println(item.objectName());

                        }

                        assertTrue(allBlobArrayFiles.contains("invalid_hosted_video_post/DASH_720.mp4"));
                        assertTrue(allBlobArrayFiles.contains("invalid_hosted_video_post/DASH_AUDIO_128.mp4"));
                        assertTrue(allBlobArrayFiles.contains("invalid_hosted_video_post/hosted_video.mpd"));

                        // Manually throwing because this logic branch should not get triggered:
                        assertEquals(true, false);

                } else {
                    int updatedStaticFileTypeResponse = SubredditTablesDB.updateStaticFileType(conn, post.getId(), "unknown");
                    if (updatedStaticFileTypeResponse != 1) {
                        System.out.printf("Error in updating static file type. Result integer returned is: %d \n", updatedStaticFileTypeResponse);
                    } else {
                        System.out.printf("Sucessfully set the static file type for post %s to unknown", post.getId());
                    }
                }
            }

            // QA-ing the subreddit post fields:
            SubredditPost fullyCompletePost = SubredditTablesDB.getPost(conn, "invalid_hosted_video_post");
            assertEquals("invalid_hosted_video_post", fullyCompletePost.getId());
            assertEquals("invalid_hosted_video_post/post.json", fullyCompletePost.getJsonPostPath());
            assertEquals("invalid_hosted_video_post/screenshot.png", fullyCompletePost.getScreenshotPath());
            assertEquals("unknown", fullyCompletePost.getStaticFileType());
            assertEquals("NorthKoreaPics", fullyCompletePost.getSubreddit());
            assertEquals(null, fullyCompletePost.getStaticRootPath());
            assertEquals(false, fullyCompletePost.isStaticDownloaded());

        }
    }


}
