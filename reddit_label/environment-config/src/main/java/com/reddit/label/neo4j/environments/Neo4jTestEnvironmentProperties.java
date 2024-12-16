package com.reddit.label.neo4j.environments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Neo4jTestEnvironmentProperties implements Neo4jEnvironment {

    public void loadEnvironmentVariablesFromFile(String pathtoFile) throws IOException {
        File file = new File(pathtoFile);

        BufferedReader reader = new BufferedReader(new FileReader(file));

        String envLine;

        while ((envLine = reader.readLine()) != null) {
            System.out.println(envLine);

            if (envLine.contains("=")) {
                String[] envLineParts = envLine.split("=");
                String name = String.format("graph.%s", envLineParts[0]);
                String value = envLineParts[1];

                if ((System.getenv(name) != null)) {
                    System.out.printf("Environment variable %s already exists....Overwriting it \n", name);
                } else {
                    System.out.printf("Environment variable %s does not exist....Overwriting it \n", name);
                }
                System.setProperty(name, value);
            } else { 
                continue;
            }
        }

        reader.close();

    }

    public String getUrl() {
        return System.getProperty("graph.NEO4J_TEST_URL");
    }

    public String getPort() {
        return System.getProperty("graph.NEO4J_TEST_PORT");
    }

    public String getDatabaseName() {
        return System.getProperty("graph.NEO4J_TEST_DB_NAME");

    }

    public String getUsername() {
        return System.getProperty("graph.NEO4J_TEST_USERNAME");
    }

    public String getPassword() {
        return System.getProperty("graph.NEO4J_TEST_PASSWORD");

    }

}