package com.reddit.label.neo4j.environments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class Neo4jTestEnvironmentPropertiesTest {
    @Test
    void testLoadTestEnvironmentVariablesFromFile() throws IOException {

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("localhost", neo4jEnvironment.getUrl());
        assertEquals("7687", neo4jEnvironment.getPort());
        assertEquals("test_db_name", neo4jEnvironment.getDatabaseName());
        assertEquals("neo4j", neo4jEnvironment.getUsername());
        assertEquals("test_password", neo4jEnvironment.getPassword());

    }

    @Test
    void testLoadDevEnvironmentVariablesFromFile() throws IOException {
        Neo4jDevEnvironmentProperties neo4jEnvironment = new Neo4jDevEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("dev_url", neo4jEnvironment.getUrl());
        assertEquals("dev_port", neo4jEnvironment.getPort());
        assertEquals("dev_db_name", neo4jEnvironment.getDatabaseName());
        assertEquals("dev_username", neo4jEnvironment.getUsername());
        assertEquals("dev_password", neo4jEnvironment.getPassword());

    }

    @Test
    void testLoadProdEnvironmentVariablesFromFile() throws IOException {

        Neo4jProdEnvironmentProperties neo4jEnvironment = new Neo4jProdEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        assertEquals("prod_url", neo4jEnvironment.getUrl());
        assertEquals("prod_port", neo4jEnvironment.getPort());
        assertEquals("prod_db_name", neo4jEnvironment.getDatabaseName());
        assertEquals("prod_username", neo4jEnvironment.getUsername());
        assertEquals("prod_password", neo4jEnvironment.getPassword());

    }


}
