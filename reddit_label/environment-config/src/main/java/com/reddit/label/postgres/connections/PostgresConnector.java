package com.reddit.label.postgres.connections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.reddit.label.postgres.environments.PostgresEnvironment;

public class PostgresConnector implements PostgresConnection {

    public String username;
    public String password;
    public String url;
    public String port;
    public String databaseName;
    public String jdbcUrl;

    public void loadEnvironment(PostgresEnvironment postgresEnvironment) {
        username = postgresEnvironment.getUsername();
        password = postgresEnvironment.getPassword();
        url = postgresEnvironment.getUrl();
        port = postgresEnvironment.getPort();
        databaseName = postgresEnvironment.getDatabase();
        jdbcUrl= String.format("jdbc:postgresql://%s:%s/%s", url, port, databaseName);
    }

    public Connection getConnection() throws SQLException {
        Properties props = new Properties();

        props.setProperty("user", username);
        props.setProperty("password", password);

        Connection conn = DriverManager.getConnection(jdbcUrl, props);

        return conn;

    }

    public String getUrl() {
        return jdbcUrl;
    }

}
