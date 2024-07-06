package com.reddit.label.GraphIngestor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;

public class SubredditPostGraphIngestorTest {

    private Driver driver;

    @BeforeEach
    void setUp() {
        driver = DB.connectTestGraphB();
    }

    
    @AfterEach
    void tearDown() {
        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
            System.out.println("Clear the neo4j database");
        } finally {
            driver.close();
        }
    }
    


    @Test
    void testIngestSubredditPostVideo() {

        SubredditPost examplePost = new SubredditPost(
            "This_is_an_example_id",
            "Example_Subreddit",
            "https://example_url.com",
            true,
            "This_is_an_example_id/screenshot.png",
            "This_is_an_example_id/post.json",
            new Timestamp(System.currentTimeMillis()), 
            "This_is_an_example_id/",
            "hosted:video"
        );

        RedditPostGraphIngestionResponse graphIngestionResponse = SubredditPostGraphIngestor.IngestSubredditPostVideo(examplePost, driver);

        assertEquals("This_is_an_example_id/post.json", graphIngestionResponse.getJsonFileName());
        assertEquals("This_is_an_example_id", graphIngestionResponse.getRedditPostId());
        assertEquals("This_is_an_example_id/screenshot.png", graphIngestionResponse.getScreenshotFileName());
        assertEquals("This_is_an_example_id/hosted_video.mpd", graphIngestionResponse.getVideoFileName());

    }
}
