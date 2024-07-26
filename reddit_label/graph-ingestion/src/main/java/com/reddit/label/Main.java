package com.reddit.label;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;

import com.reddit.label.BlobStorage.MinioClientConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.GraphIngestor.RedditPostGraphIngestionResponse;
import com.reddit.label.GraphIngestor.SubredditPostGraphIngestor;

import io.minio.MinioClient;

public class Main {

    public static void main(String[] args) throws SQLException {

        // Getting all of the subreddit posts where the static data has been ingested and the file type is
        Connection conn = DB.connect();
        Driver driver = DB.connectGraphDB();
        MinioClient minioClient = MinioClientConfig.getMinioClient();

        List<SubredditPost> postsToIngest = SubredditTablesDB.getPostsBasedOnStaticFileType(conn, "hosted:video");
        
        // Getting list of Ids already in Graph database:
        List<String> existingGraphPosts = new ArrayList<>();
        var result = driver.executableQuery("MATCH (n:Entity:RedditPost) return n.id").execute();
        var records = result.records();
        records.forEach(r -> {
            existingGraphPosts.add(r.get("id").asString());
        });
        
        List<SubredditPost> newPostsToIngest = postsToIngest.stream().filter(post -> !existingGraphPosts.contains(post.getId())).collect(Collectors.toList());

        // Iterate through each post and ingests the item into the Neo4J database: 
        for (SubredditPost post: newPostsToIngest) {

            System.out.printf("Beginning to insert reddit post %s to graph database\n", post.getId());
            try {
                RedditPostGraphIngestionResponse postIngestionResponse = SubredditPostGraphIngestor.IngestSubredditPostVideo(post, minioClient, driver);
                System.out.printf("Ingested the subreddit post %s into the graph database:  \n", postIngestionResponse.getRedditPostId());

            } catch (Exception e) {
                System.out.printf("Error in ingesting reddit post %s \n", post.getId());
                e.printStackTrace();
            }
        }

    }
}
