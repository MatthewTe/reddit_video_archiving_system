package com.reddit.label.Databases;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;

import com.reddit.label.neo4j.connections.Neo4jConnector;
import com.reddit.label.neo4j.environments.Neo4jTestEnvironmentProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.parameters;

import java.io.IOException;

public class DBTest {

    private Driver driver;

    @BeforeEach
    void setUp() throws IOException {

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        driver = neo4jConnector.getDriver();
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
    void testConnectTestGraphB() {
        try (var session = driver.session()) {

            var query = new Query(
                "CREATE (a:Greeting) SET a.message = $message RETURN a.message", 
                parameters("message", "Hello World")
            );

            var result = session.run(query).single().get(0).asString();

            System.out.println(result);

            assertEquals("Hello World", result);
        }
    }
}
