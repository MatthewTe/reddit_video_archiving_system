package com.reddit.label.neo4j.connections;

import org.neo4j.driver.Driver;

import com.reddit.label.neo4j.environments.Neo4jEnvironment;

public interface Neo4jConnection {

    public void loadEnvironment(Neo4jEnvironment neo4jEnvironment);

    public Driver getDriver();

    public String getURI();

}
