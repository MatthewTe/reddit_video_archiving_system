package com.reddit.label.postgres.environments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class PostgresDevEnvironmentProperties implements PostgresEnvironment {
    
    public void loadEnvironmentVariablesFromFile(String pathToEnvFile) throws IOException {

        File envFile = new File(pathToEnvFile);

        BufferedReader reader = new BufferedReader(new FileReader(envFile));

        String envLine;

        while ((envLine = reader.readLine()) != null) {

            if (envLine.contains("=")) {
                String[] envLineParts = envLine.split("=");
                String name = String.format("db.%s", envLineParts[0]);
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

    public String getUsername() {
        return System.getProperty("db.POSTGRES_DEV_DB_USERNAME");
    }

    public String getPassword() {
        return System.getProperty("db.POSTGRES_DEV_DB_PASSWORD");
    }
    public String getUrl() {
        return System.getProperty("db.POSTGRES_DEV_DB_URL");
    }
    public String getPort() {
        return System.getProperty("db.POSTGRES_DEV_DB_PORT");
    }
    public String getDatabase() {
        return System.getProperty("db.POSTGRES_DEV_DB_DATABASE");
    }

}
