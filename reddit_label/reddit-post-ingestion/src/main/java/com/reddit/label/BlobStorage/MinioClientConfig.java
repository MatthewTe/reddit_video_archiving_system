package com.reddit.label.BlobStorage;

import io.minio.MinioClient;

public class MinioClientConfig {
    /*
     * Returns the various configurations of the MinioClient. Pulls the configuration params
     * from the varios Config objects. Serves as an abstraction layer over the Config objects that
     * pull various params from the running environment.
     */

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
