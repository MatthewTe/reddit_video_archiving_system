package com.reddit.label.GraphIngestor;

public class LoopTTArticleGraphIngestor {
    public static void MigrateLoopTTArticlesToGraphDB(Connection conn, Driver driver) {

        // Grab all of the ids in the neo4j database.
        // Grab all of the posts from the postgres database that aren't in the existing list.
        // Iterate through all of the articles and insert them into the neo4j Database.


    }
}
