package com.reddit.label.minio.environments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class MinioTestEnvironmentPropertiesTest {

    @Test
    void testLoadTestingEnvironmentVariablesFromFile() throws IOException {

        MinioTestEnvironmentProperties minioEnvironment = new MinioTestEnvironmentProperties();
        minioEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("localhost", minioEnvironment.getUrl());
        assertEquals("9001", minioEnvironment.getPort());
        assertEquals("test_username", minioEnvironment.getUsername());
        assertEquals("test_password", minioEnvironment.getPassword());
        assertEquals("test_user_id", minioEnvironment.getUserId());
        assertEquals("test_secret_key", minioEnvironment.getSecretKey());

    }

    @Test
    void testLoadDevEnvironmentVariablesFromFile() throws IOException {

        MinioDevEnvironmentProperties minioEnvironment = new MinioDevEnvironmentProperties();
        minioEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("dev_url", minioEnvironment.getUrl());
        assertEquals("dev_port", minioEnvironment.getPort());
        assertEquals("dev_username", minioEnvironment.getUsername());
        assertEquals("dev_password", minioEnvironment.getPassword());
        assertEquals("dev_user_id", minioEnvironment.getUserId());
        assertEquals("dev_secret_key", minioEnvironment.getSecretKey());

    }

    @Test
    void testLoadProdEnvironmentVariablesFromFile() throws IOException {

        MinioProdEnvironmentProperties minioEnvironment = new MinioProdEnvironmentProperties();
        minioEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("prod_url", minioEnvironment.getUrl());
        assertEquals("prod_port", minioEnvironment.getPort());
        assertEquals("prod_username", minioEnvironment.getUsername());
        assertEquals("prod_password", minioEnvironment.getPassword());
        assertEquals("prod_user_id", minioEnvironment.getUserId());
        assertEquals("prod_secret_key", minioEnvironment.getSecretKey());

    }

}
