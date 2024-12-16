package com.reddit.label.neo4j.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import com.reddit.label.neo4j.environments.Neo4jTestEnvironmentProperties;

public class Neo4jConnectorTest {

    @Test
    void testGetURI() throws IOException {

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        assertEquals("neo4j://localhost:7687", neo4jConnector.getURI());

    }

    @Test
    void testGetDriver() throws IOException {

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        assertTrue(neo4jConnector.getDriver() instanceof Driver);

        Boolean isConnected;
        try (Driver driver = neo4jConnector.getDriver(); Session session = driver.session()) {
            @SuppressWarnings("unused")
            Result result = session.run("RETURN 1");
            isConnected = true;
        } catch (Exception e) {
            e.printStackTrace();
            isConnected = false;
        }

        assertTrue(isConnected);

    }
}
