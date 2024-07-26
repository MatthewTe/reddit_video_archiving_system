package com.reddit.label;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.neo4j.driver.Driver;

import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.GraphIngestor.RedditPostGraphIngestionResponse;
import com.reddit.label.GraphIngestor.SubredditPostGraphIngestor;
import com.reddit.label.minio.connections.MinioHttpConnector;
import com.reddit.label.minio.environments.MinioProdEnvironmentProperties;
import com.reddit.label.neo4j.connections.Neo4jConnector;
import com.reddit.label.neo4j.environments.Neo4jProdEnvironmentProperties;
import com.reddit.label.postgres.connections.PostgresConnector;
import com.reddit.label.postgres.environments.PostgresProdEnvironmentProperties;

import io.minio.MinioClient;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        Options options = new Options();
        options.addOption("e", "env-file", true, "Pointing to a path to a production environment file that will be used to run the ingestor");
        
        String envFilePath = "";

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            envFilePath = cmd.getOptionValue("env-file");
        } catch (ParseException e) {
            System.err.println("Error with parsing command lines" + e.getMessage());
            System.exit(1);
        }

        PostgresProdEnvironmentProperties postgresEnvironment = new PostgresProdEnvironmentProperties();
        postgresEnvironment.loadEnvironmentVariablesFromFile(envFilePath);
        PostgresConnector psqlConnector = new PostgresConnector();
        psqlConnector.loadEnvironment(postgresEnvironment);

        // Getting all of the subreddit posts where the static data has been ingested and the file type is
        Connection conn = psqlConnector.getConnection();

        Neo4jProdEnvironmentProperties neo4jEnvironment = new Neo4jProdEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile(envFilePath);
        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        Driver driver = neo4jConnector.getDriver();

        MinioProdEnvironmentProperties minioProdEnvironment = new MinioProdEnvironmentProperties();
        minioProdEnvironment.loadEnvironmentVariablesFromFile(envFilePath);
        MinioHttpConnector minioConnector = new MinioHttpConnector();
        minioConnector.loadEnvironment(minioProdEnvironment);

        MinioClient minioClient = minioConnector.getClient();

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
