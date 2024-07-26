package com.reddit.label.postgres.environments;

public interface PostgresEnvironment {

    String getUsername();
    String getPassword();
    String getUrl();
    String getPort();
    String getDatabase();
}
