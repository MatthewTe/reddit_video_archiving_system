package com.reddit.label.minio.environments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MinioTestEnvironmentProperties implements MinioEnvironment {

    public void loadEnvironmentVariablesFromFile(String pathToEnvFile) throws IOException {

        File envFile = new File(pathToEnvFile);
        BufferedReader reader = new BufferedReader(new FileReader(envFile));

        String envLine;
        while ((envLine = reader.readLine()) != null) {
            System.out.println(envLine);

            if (envLine.contains("=")) {
                String[] envLineParts = envLine.split("=");
                String name = String.format("blob.%s", envLineParts[0]);
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
        return System.getProperty("blob.MINIO_TEST_URL");
    }

    public String getPort() {
        return System.getProperty("blob.MINIO_TEST_PORT");
    }
    public String getUsername() {
        return System.getProperty("blob.MINIO_TEST_USERNAME");
    }

    public String getPassword() {
        return System.getProperty("blob.MINIO_TEST_PASSWORD");
    }

    public String getUserId() {
        return System.getProperty("blob.MINIO_TEST_USER_ID");
    }
    public String getSecretKey() {
        return System.getProperty("blob.MINIO_TEST_SECRET_KEY");
    }

}
