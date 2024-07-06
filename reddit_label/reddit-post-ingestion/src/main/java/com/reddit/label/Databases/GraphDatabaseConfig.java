package com.reddit.label.Databases;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GraphDatabaseConfig {

    private static final Properties properties = new Properties();

    static {
        try (InputStream input = DatabaseConfig.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                System.out.println("Unable to find db.properties");
                System.exit(1);
            }

            properties.load(input);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getDbUrl() {
        return properties.getProperty("neo4j.url");
    }

    public static String getDbUsername() {
        return properties.getProperty("neo4j.username");
    }

    public static String getDbPassword() {
        return properties.getProperty("neo4j.password");
    }

    public static String getTestDbUrl() {
        return properties.getProperty("neo4j.test.url");
    }

    public static String getTestDbUsername() {
        return properties.getProperty("neo4j.test.username");
    }
    
    public static String getTestDbPassword() {
        return properties.getProperty("neo4j.test.password");
    }

}
