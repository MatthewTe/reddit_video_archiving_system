package com.reddit.label.Databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DB {
    public static Connection connect() throws SQLException {

        try {

            var jdbcUrl = DatabaseConfig.getDbUrl();
            var username = DatabaseConfig.getDbUsername();
            var password = DatabaseConfig.getDbPassword();

            System.out.println("Ceated connection to the postgres database");

            return DriverManager.getConnection(jdbcUrl, username, password);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return null;
        }

    }

    public static Connection connectTestDB() throws SQLException {
        
        try {
            var jdbcUrl = DatabaseConfig.getTestDbUrl();
            var username = DatabaseConfig.getTestDbUsername();
            var password = DatabaseConfig.getTestDbPassword();

            System.out.println("Created connection to the test postgres database");

            return DriverManager.getConnection(jdbcUrl, username, password);

        }  catch (SQLException e) {
            System.err.println(e.getMessage());
            return null;
        }

    }
}
