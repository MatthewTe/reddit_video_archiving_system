package com.reddit.label.reddit.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class RedditTestEnvironmentPropertiesTest {
    @Test
    void testLoadTestEnvironmentFromFile() throws IOException {

        RedditTestEnvironmentProperties redditEnvironment = new RedditTestEnvironmentProperties();
        redditEnvironment.loadEnvironmentFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("test_username", redditEnvironment.getUsername());
        assertEquals("test_password", redditEnvironment.getPassword());

    }

    @Test
    void testLoadDevEnvironmentFromFile() throws IOException {

        RedditDevEnvironmentProperties redditEnvironment = new RedditDevEnvironmentProperties();
        redditEnvironment.loadEnvironmentFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("dev_username", redditEnvironment.getUsername());
        assertEquals("dev_password", redditEnvironment.getPassword());

    }
    @Test
    void testLoadProdEnvironmentFromFile() throws IOException {

        RedditProdEnvironmentProperties redditEnvironment = new RedditProdEnvironmentProperties();
        redditEnvironment.loadEnvironmentFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("prod_username", redditEnvironment.getUsername());
        assertEquals("prod_password", redditEnvironment.getPassword());

    }


}
