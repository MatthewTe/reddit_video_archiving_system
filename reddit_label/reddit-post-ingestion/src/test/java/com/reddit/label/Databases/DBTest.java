package com.reddit.label.Databases;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.parameters;

public class DBTest {

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
