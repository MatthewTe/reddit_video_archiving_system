package com.reddit.label;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.neo4j.driver.Driver;

import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.GraphIngestor.RedditPostGraphIngestionResponse;
import com.reddit.label.GraphIngestor.SubredditPostGraphIngestor;

public class Main {

    public static void main(String[] args) throws SQLException {

        // Getting all of the subreddit posts where the static data has been ingested and the file type is
        Connection conn = DB.connect();
        Driver driver = DB.connectGraphDB();

        List<SubredditPost> postsToIngest = SubredditTablesDB.getPostsBasedOnStaticFileType(conn, "hosted:video");

        // Iterate through each post and ingests the item into the Neo4J database: 
        for (SubredditPost post: postsToIngest) {

            System.out.printf("Beginning to insert reddit post %s to graph database\n", post.getId());
            try {
                RedditPostGraphIngestionResponse postIngestionResponse = SubredditPostGraphIngestor.IngestSubredditPostVideo(post, driver);
                System.out.printf("Ingested the subreddit post %s into the graph database:  \n", postIngestionResponse.getRedditPostId());

            } catch (Exception e) {
                System.out.printf("Error in ingesting reddit post %s \n", post.getId());
                e.printStackTrace();
            }


        }

    }
}
