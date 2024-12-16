package com.reddit.label.postgres.environments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PostgresEnvironmentPropertiesTest {

    @Test
    void TestLoadingDevEnvironmentVariablesFromDotEnvFile() {
        PostgresDevEnvironmentProperties psqlEnvironment = new PostgresDevEnvironmentProperties();
        
        try {
            psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

            assertEquals("dev_username", psqlEnvironment.getUsername());
            assertEquals("dev_password", psqlEnvironment.getPassword());
            assertEquals("dev_url", psqlEnvironment.getUrl());
            assertEquals("5432", psqlEnvironment.getPort());
            assertEquals("dev_database", psqlEnvironment.getDatabase());

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    @Test
    void TestLoadingTestingEnvironmentVariablesFromDotEnvFile() {
        PostgresTestEnvironmentProperties psqlEnvironment = new PostgresTestEnvironmentProperties();
        
        try {
            psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

            assertEquals("test_username", psqlEnvironment.getUsername());
            assertEquals("test_password", psqlEnvironment.getPassword());
            assertEquals("localhost", psqlEnvironment.getUrl());
            assertEquals("5432", psqlEnvironment.getPort());
            assertEquals("test_database", psqlEnvironment.getDatabase());

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    @Test
    void TestLoadingProdEnvironmentVariablesFromDotEnvFile() {
        PostgresProdEnvironmentProperties psqlEnvironment = new PostgresProdEnvironmentProperties();
        
        try {
            psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

            assertEquals("prod_username", psqlEnvironment.getUsername());
            assertEquals("prod_password", psqlEnvironment.getPassword());
            assertEquals("prod_url", psqlEnvironment.getUrl());
            assertEquals("5432", psqlEnvironment.getPort());
            assertEquals("prod_database", psqlEnvironment.getDatabase());

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}


