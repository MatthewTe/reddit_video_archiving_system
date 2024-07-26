package com.reddit.label.neo4j.environments;

public interface Neo4jEnvironment {
    public String getUrl();
    public String getPort();
    public String getDatabaseName();
    public String getUsername();
    public String getPassword();
}
