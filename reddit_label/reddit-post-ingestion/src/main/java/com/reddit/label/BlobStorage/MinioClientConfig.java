package com.reddit.label.BlobStorage;

import io.minio.MinioClient;

public class MinioClientConfig {

    public static MinioClient getMinioClient() {
        return MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioEndpoint())
            .credentials(BlobStorageConfig.getMinioUserId(), BlobStorageConfig.getMinioAccesskey())
            .build();
    }

    public static MinioClient geTestMinioClient() {
        return MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint())
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();
    }

}
