package com.reddit.label.postgres.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.reddit.label.postgres.environments.PostgresTestEnvironmentProperties;

public class PostgresConnectorTest {

    @Test
    void testGetConnection() throws SQLException {

        PostgresTestEnvironmentProperties psqlEnvironment = new PostgresTestEnvironmentProperties();
        try {
            psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        } catch (Exception e) {
            e.printStackTrace();;
            return;
        }

        PostgresConnector testConnector = new PostgresConnector();
        testConnector.loadEnvironment(psqlEnvironment);

        assertTrue(testConnector.getConnection() instanceof Connection);

        Boolean isConnected;
        try (Connection conn = testConnector.getConnection()) {

            try (PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
                ResultSet result = stmt.executeQuery();
                if (result.next()) {
                    isConnected = true;
                } else {
                    isConnected = false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            isConnected = false;
        }

        assertTrue(isConnected);
        
    }

    @Test
    void testGetUrl() {

        PostgresTestEnvironmentProperties psqlEnvironment = new PostgresTestEnvironmentProperties();
        try {
            psqlEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        } catch (Exception e) {
            e.printStackTrace();;
            return;
        }

        PostgresConnector testConnector = new PostgresConnector();
        testConnector.loadEnvironment(psqlEnvironment);

        assertEquals("jdbc:postgresql://localhost:5432/test_database", testConnector.getUrl());

    }
}
