package com.reddit.label.neo4j.connections;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import com.reddit.label.neo4j.environments.Neo4jEnvironment;

public class Neo4jConnector implements Neo4jConnection {

    public String neo4jUrl;
    public String neo4jUsername;
    public String neo4jPassword;
    public String neo4jDatabase;
    public String neo4jPort;
    public String neo4jUri;

    public void loadEnvironment(Neo4jEnvironment neo4jEnvironment) {

        neo4jUrl = neo4jEnvironment.getUrl();
        neo4jUsername = neo4jEnvironment.getUsername();
        neo4jPassword = neo4jEnvironment.getPassword();
        neo4jDatabase = neo4jEnvironment.getDatabaseName();
        neo4jPort = neo4jEnvironment.getPort();
        
        neo4jUri = String.format("neo4j://%s:%s", neo4jUrl, neo4jPort);
    }

    public Driver getDriver() {
        return GraphDatabase.driver(
            neo4jUri,
            AuthTokens.basic(neo4jUsername, neo4jPassword)
        );
    }

    public String getURI() {
        return neo4jUri;
    }

}
