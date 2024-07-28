package com.reddit.label.minio.connections;

import com.reddit.label.minio.environments.MinioEnvironment;

import io.minio.MinioClient;

public class MinioHttpConnector implements MinioConnection {

    public String minioUrl;
    public String minioPort;
    public String minioUserId;
    public String minioAccessKey;

    public String minioFullEndpoint;
    
    public void loadEnvironment(MinioEnvironment environment) {

        minioUrl = environment.getUrl();
        minioPort = environment.getPort();
        minioUserId = environment.getUserId();
        minioAccessKey = environment.getSecretKey();

        minioFullEndpoint = String.format("http://%s:%s", minioUrl, minioPort);
    }

    public MinioClient getClient() {
        return MinioClient.builder()
            .endpoint(minioFullEndpoint)
            .credentials(minioUserId, minioAccessKey)
            .build();
    }

    public String getEndpoint() {
        return minioFullEndpoint;
    }

    

}
