package com.reddit.label.minio.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.reddit.label.minio.environments.MinioTestEnvironmentProperties;

import io.minio.MinioClient;

public class MinioHttpConnectorTest {
    @Test
    void testLoadEnvironment() throws IOException {
        MinioTestEnvironmentProperties minioEnvironment = new MinioTestEnvironmentProperties();
        minioEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        MinioHttpConnector minioConnection = new MinioHttpConnector();
        minioConnection.loadEnvironment(minioEnvironment);

        assertEquals("http://localhost:9000", minioConnection.getEndpoint());
    }

    @Test
    void testgetClient() throws Exception {
        MinioTestEnvironmentProperties minioEnvironment = new MinioTestEnvironmentProperties();
        minioEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");

        MinioHttpConnector minioConnection = new MinioHttpConnector();
        minioConnection.loadEnvironment(minioEnvironment);

        assertTrue(minioConnection.getClient() instanceof MinioClient);

        List<?> bucketList = minioConnection.getClient().listBuckets();
        if (bucketList == null) {
            System.out.println("Bucketlist is null. Connection to the minio client failed.");
            throw new Exception("Bucketlist is null. Connection to the minio client failed.");
        } else {
            System.out.println(bucketList);
        }

    }
}
