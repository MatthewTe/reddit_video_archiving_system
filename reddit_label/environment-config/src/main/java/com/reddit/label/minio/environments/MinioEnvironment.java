package com.reddit.label.minio.environments;

public interface MinioEnvironment {
    
    public String getUrl();
    public String getPort();
    public String getUsername();
    public String getPassword();
    public String getUserId();
    public String getSecretKey();

}
