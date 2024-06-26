package com.reddit.label.StaticFileIngestors;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;

import com.fasterxml.jackson.databind.JsonNode;
import com.reddit.label.RedditPostJsonDefaultAttributes;
import com.reddit.label.Databases.SubredditPost;

import io.minio.MinioClient;

public interface StaticFileIngestor {
    public String fileToBlob(SubredditPost redditPost, RedditPostJsonDefaultAttributes postDefaultAttributes, JsonNode postNode, Connection con, MinioClient minioClient) throws MalformedURLException, IOException;
}
