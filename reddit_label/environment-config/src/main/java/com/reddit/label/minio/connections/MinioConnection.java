package com.reddit.label.minio.connections;

import com.reddit.label.minio.environments.MinioEnvironment;

import io.minio.MinioClient;

public interface MinioConnection {

    public void loadEnvironment(MinioEnvironment environment);

    public MinioClient getClient();
    public String getEndpoint();

}
