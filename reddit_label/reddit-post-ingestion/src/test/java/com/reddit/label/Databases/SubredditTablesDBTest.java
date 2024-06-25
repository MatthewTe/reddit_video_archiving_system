package com.reddit.label.Databases;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubredditTablesDBTest {

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
    void testInsertBasicSubredditPost() throws SQLException {
        
        List<SubredditPost> posts = new ArrayList<>();
        posts.add(new SubredditPost("id1", "subreddit1", "url1", true, "screenshotPath1", "jsonPostPath1", null, "staticRootPath1", "testPath"));
        posts.add(new SubredditPost("id2", "subreddit2", "url2", false, "screenshotPath2", "jsonPostPath2", null, "staticRootPath2", "testPath"));

        int insertedRecords = SubredditTablesDB.InsertBasicSubredditPost(conn, posts);

        assertEquals(posts.size(), insertedRecords);

        SubredditPost post1 = SubredditTablesDB.getPost(conn, "id1");
        SubredditPost post2 = SubredditTablesDB.getPost(conn, "id2");

        assertEquals(post1.getId(), "id1");
        assertEquals(post2.getId(), "id2");

    }

    @Test
    void testGetPost() {

        List<SubredditPost> posts = new ArrayList<>();
        posts.add(new SubredditPost(
            "this_is_the_test_id", 
            "subreddit_test",
            "url_test",
            true,
            "test_screenshot_path", 
            "test_json_path", 
            null,
            "test_staticroot_path",
            "test_static_file_type"
        ));

        int insertedSubreddit = SubredditTablesDB.InsertFullSubredditPost(conn, posts);
        assertEquals(insertedSubreddit, 1); 


        SubredditPost postFromDatabase = SubredditTablesDB.getPost(conn, "this_is_the_test_id");

        assertEquals(postFromDatabase.getId(), "this_is_the_test_id");
        assertEquals(postFromDatabase.getSubreddit(), "subreddit_test");
        assertEquals(postFromDatabase.getJsonPostPath(), "test_json_path");
        assertEquals(postFromDatabase.getScreenshotPath(), "test_screenshot_path");
        assertEquals(postFromDatabase.getStaticRootPath(), "test_staticroot_path");
        assertEquals(postFromDatabase.getUrl(), "url_test");
        assertEquals(postFromDatabase.getStaticFileType(), "test_static_file_type");
    }

    @Test
    void testUpdateSubredditJSON() {

        List<SubredditPost> jsonPosts = new ArrayList<>();
        jsonPosts.add(new SubredditPost("id_for_test_json", "", "", true, "screenshotPath1", "inital_json_path", null, "staticRootPath1", "staticFileType"));

        int insertedSubreddit = SubredditTablesDB.InsertFullSubredditPost(conn, jsonPosts);
        assertEquals(insertedSubreddit, 1);

        SubredditPost subredditFromDatabase = SubredditTablesDB.getPost(conn, "id_for_test_json");
        assertEquals(subredditFromDatabase.getJsonPostPath(), "inital_json_path");

        int updateQueryResult = SubredditTablesDB.updateSubredditJSON(conn, "id_for_test_json", "a_new_json_path");
        assertEquals(updateQueryResult, 1);       

        SubredditPost subredditFromDatabaseUpdatedJson = SubredditTablesDB.getPost(conn, "id_for_test_json");
        assertEquals(subredditFromDatabaseUpdatedJson.getJsonPostPath(), "a_new_json_path");
    }

    @Test
    void testUpdateSubredditPostScreenshot() {

        List<SubredditPost> screenshotPosts = new ArrayList<>();
        screenshotPosts.add(new SubredditPost("id_for_test_screenshot", "", "", true, "initial_screenshot_path", "", null, "staticRootPath1", "staticFileType"));

        int insertedSubreddit = SubredditTablesDB.InsertFullSubredditPost(conn, screenshotPosts);
        assertEquals(insertedSubreddit, 1);

        SubredditPost subredditFromDatabase = SubredditTablesDB.getPost(conn, "id_for_test_screenshot");
        assertEquals(subredditFromDatabase.getScreenshotPath(), "initial_screenshot_path");

        int updateQueryResult = SubredditTablesDB.updateSubredditPostScreenshot(conn, "id_for_test_screenshot", "a_new_screenshot_path");
        assertEquals(updateQueryResult, 1);       

        SubredditPost subredditFromDatabaseUpdatedScreenshot = SubredditTablesDB.getPost(conn, "id_for_test_screenshot");
        assertEquals(subredditFromDatabaseUpdatedScreenshot.getScreenshotPath(), "a_new_screenshot_path");

    }

    @Test
    void testUpdateSubredditPostStaicPath() {

        List<SubredditPost> statcPathPosts = new ArrayList<>();
        statcPathPosts.add(new SubredditPost("id_for_test_static_path", "", "", true, "", "", null, "inital_static_root_path", "staticFileType"));

        int insertedSubreddit = SubredditTablesDB.InsertFullSubredditPost(conn, statcPathPosts);
        assertEquals(insertedSubreddit, 1);

        SubredditPost subredditFromDatabase = SubredditTablesDB.getPost(conn, "id_for_test_static_path");
        assertEquals(subredditFromDatabase.getStaticRootPath(), "inital_static_root_path");

        int updateQueryResult = SubredditTablesDB.updateSubredditPostStaticPath(conn, "id_for_test_static_path", "a_new_static_path");
        assertEquals(updateQueryResult, 1);       

        SubredditPost subredditFromDatabaseUpdatedStaticFile = SubredditTablesDB.getPost(conn, "id_for_test_static_path");
        assertEquals(subredditFromDatabaseUpdatedStaticFile.getStaticRootPath(), "a_new_static_path");

    }

    @Test
    void testUpdateStaticDownloadedFlagTrue() {

        List<SubredditPost> staticFlagUpdatePosts = new ArrayList<>();
        // String id, String subreddit, String url, boolean staticDownloaded, String screenshotPath
        staticFlagUpdatePosts.add(new SubredditPost(
            "id_testing_for_static_flag",
            "TestSubreddit",
            "test_url",
            false
        ));

        int insertedSubreddit = SubredditTablesDB.InsertBasicSubredditPost(conn, staticFlagUpdatePosts);
        assertEquals(insertedSubreddit, 1);

        SubredditPost postFromDB = SubredditTablesDB.getPost(conn, "id_testing_for_static_flag");
        assertEquals(postFromDB.getId(), "id_testing_for_static_flag");
        assertEquals(postFromDB.getSubreddit(), "TestSubreddit");
        assertEquals(postFromDB.getUrl(), "test_url");
        assertEquals(postFromDB.isStaticDownloaded(), false);

        int updatedStaticDownloadedFlagResult = SubredditTablesDB.updateStaticDownloadedFlagTrue(conn, postFromDB.getId());
        assertEquals(updatedStaticDownloadedFlagResult, 1);

        SubredditPost updatedPost = SubredditTablesDB.getPost(conn, "id_testing_for_static_flag");
        assertEquals(updatedPost.getId(), "id_testing_for_static_flag");
        assertEquals(updatedPost.isStaticDownloaded(),true); 

    }

    @Test
    void testUpdateStaticFileType() {
        List<SubredditPost> staticFlagUpdatePosts = new ArrayList<>();
        SubredditPost updatedFlagSubredditPost = new SubredditPost("id_testing_for_static_file_type");

        updatedFlagSubredditPost.setSubreddit("TestSubreddit");
        updatedFlagSubredditPost.setUrl("test_url");

        updatedFlagSubredditPost.setStaticFileType("hosted:video");
        updatedFlagSubredditPost.setStaticRootPath("id_testing_for_static_file_type");
        updatedFlagSubredditPost.setStaticDownloaded(true);
        
        staticFlagUpdatePosts.add(updatedFlagSubredditPost);

        int insertedSubreddit = SubredditTablesDB.InsertBasicSubredditPost(conn, staticFlagUpdatePosts);
        assertEquals(1, insertedSubreddit);


        int insertedPost = SubredditTablesDB.updateStaticFields(conn, updatedFlagSubredditPost);
        assertEquals(insertedPost, 1);

        SubredditPost updatedStaticFieldsPost = SubredditTablesDB.getPost(conn, "id_testing_for_static_file_type");

        assertEquals("id_testing_for_static_file_type", updatedStaticFieldsPost.getId());
        assertEquals("hosted:video", updatedStaticFieldsPost.getStaticFileType());
        assertEquals("id_testing_for_static_file_type", updatedStaticFieldsPost.getStaticRootPath());
        assertEquals(true, updatedStaticFieldsPost.isStaticDownloaded());

    }

}
