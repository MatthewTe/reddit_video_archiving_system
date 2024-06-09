package com.reddit.label.Databases;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SubredditTablesDB {
    
    public static void createSubredditTables() {
        String createSubredditPostsTable = """
                CREATE TABLE IF NOT EXISTS subreddit_posts (
                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                    subreddit VARCHAR(255),
                    url TEXT NOT NULL,
                    static_downloaded BOOLEAN NOT NULL,
                    screenshot TEXT,
                    json_post TEXT,
                    inserted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """;

        try (var conn = DB.connect();
            var stmt = conn.createStatement()) {
                stmt.executeUpdate(createSubredditPostsTable);
                System.out.println("Trying to create subreddit ingestion table");

        }  catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static int InsertBasicSubredditPost(List<SubredditPost> posts) {
        var insertStatement = "INSERT INTO subreddit_posts(id, subreddit, url, static_downloaded) VALUES(?, ?, ?, ?)";

        try (var conn = DB.connect();
            var pstmt = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS)) {
        
            conn.setAutoCommit(false);

            for (SubredditPost post: posts) {
                pstmt.setString(1,  post.getId());
                pstmt.setString(2, post.getSubreddit());
                pstmt.setString(3, post.getUrl());
                pstmt.setBoolean(4, post.isStaticDownloaded());

                pstmt.addBatch();

            }
            
            int[] result = pstmt.executeBatch();
            conn.commit();

            System.out.println("Inserted rows: " + result.length);

            return result.length;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;

    }   

    public static List<String> findAllIds() {
        var postIds = new ArrayList<String>();

        String getAllQuery = "SELECT id FROM subreddit_posts";

        try (var conn = DB.connect();
            var stmt = conn.createStatement()) {

            var rs = stmt.executeQuery(getAllQuery);
            while (rs.next()) {
               var id =  rs.getString("id");
               postIds.add(id);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return postIds;
    }

}
