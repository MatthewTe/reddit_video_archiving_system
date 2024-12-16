package com.reddit.label.postgres.connections;

import java.sql.Connection;
import java.sql.SQLException;

import com.reddit.label.postgres.environments.PostgresEnvironment;

public interface PostgresConnection {

    void loadEnvironment(PostgresEnvironment postgresEnvironment);

    Connection getConnection() throws SQLException;
    String getUrl();
}
