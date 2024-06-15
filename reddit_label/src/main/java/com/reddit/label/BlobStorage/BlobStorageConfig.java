package com.reddit.label.BlobStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BlobStorageConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = BlobStorageConfig.class.getClassLoader().getResourceAsStream("db.properties")) {

            if (input == null) {
                System.out.println("Unable to find db.properties");
                System.exit(1);
            }

            properties.load(input);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String getMinioEndpoint() {
        return properties.getProperty("minio.url");
    }
    
    public static String getMinioUserId() {
        return properties.getProperty("minio.userid");
    }

    public static String getMinioAccesskey() {
        return properties.getProperty("minio.secretkey");
    }

    public static String getMinioTestEndpoint() {
        return properties.getProperty("minio.test.url");
    }
    
    public static String getMinioTestUserId() {
        return properties.getProperty("minio.test.userid");
    }

    public static String getMinioTestAccesskey() {
        return properties.getProperty("minio.test.secretkey");
    }
}
