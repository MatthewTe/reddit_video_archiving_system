package com.reddit.label.Databases;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SubredditTablesDB {
    
    public static void createSubredditTables(Connection conn) {
        String createSubredditPostsTable = """
                CREATE TABLE IF NOT EXISTS subreddit_posts (
                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                    subreddit VARCHAR(255),
                    url TEXT NOT NULL,
                    static_downloaded BOOLEAN NOT NULL,
                    static_root_url TEXT DEFAULT NULL,
                    static_file_type VARCHAR(50) NULL,
                    screenshot TEXT,
                    json_post TEXT,
                    inserted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """;

        try (var stmt = conn.createStatement()) {
                stmt.executeUpdate(createSubredditPostsTable);
                System.out.println("Trying to create subreddit ingestion table");

        }  catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static int InsertBasicSubredditPost(Connection conn, List<SubredditPost> posts) {
        var insertStatement = "INSERT INTO subreddit_posts(id, subreddit, url, static_downloaded) VALUES(?, ?, ?, ?)";

        try (var pstmt = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS)) {
        
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

    public static int InsertFullSubredditPost(Connection conn, List<SubredditPost> posts) {
        var insertStatement = """
            INSERT INTO subreddit_posts(
                id, 
                subreddit, 
                url, 
                static_downloaded,
                static_root_url,
                screenshot,
                static_file_type,
                json_post
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (var pstmt = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS)) {
        
            conn.setAutoCommit(false);

            for (SubredditPost post: posts) {
                pstmt.setString(1,  post.getId());
                pstmt.setString(2, post.getSubreddit());
                pstmt.setString(3, post.getUrl());
                pstmt.setBoolean(4, post.isStaticDownloaded());
                pstmt.setString(5, post.getStaticRootPath());
                pstmt.setString(6, post.getScreenshotPath());
                pstmt.setString(7, post.getStaticFileType());
                pstmt.setString(8, post.getJsonPostPath());
                
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

    public static SubredditPost getPost(Connection conn, String id) {
        String idQuery = """
            SELECT 
                id, 
                subreddit, 
                url, 
                static_downloaded, 
                screenshot, 
                json_post, 
                inserted_date, 
                static_root_url,
                static_file_type,
            FROM subreddit_posts WHERE id = ?""";

        try (PreparedStatement pstmt = conn.prepareStatement(idQuery)) {
            
            pstmt.setString(1, id);

            ResultSet result = pstmt.executeQuery();

            while (result.next()) {
                SubredditPost post = new SubredditPost(
                    result.getString("id"), 
                    result.getString("subreddit"), 
                    result.getString("url"), 
                    result.getBoolean("static_downloaded"), 
                    result.getString("screenshot"), 
                    result.getString("json_post"), 
                    result.getTimestamp("inserted_date"),
                    result.getString("static_root_url"),
                    result.getString("static_file_type")
                );

                return post;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<String> getPostIdsNoStatic(Connection conn, int limit) {

        var query = "SELECT id FROM subreddit_posts WHERE static_downloaded = false LIMIT ?";
        List<String> postIds = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setInt(1, limit);

            ResultSet result = pstmt.executeQuery();

            while (result.next()) {
                postIds.add(result.getString("id"));
            }

            return postIds;


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<SubredditPost> getPostsNoStatic(Connection conn, int limit) {

        var query = """
            SELECT
                id, 
                subreddit, 
                url, 
                static_downloaded, 
                screenshot, 
                json_post, 
                inserted_date, 
                static_root_url,
                static_file_type  
            FROM subreddit_posts 
            WHERE static_downloaded = false 
            AND static_file_type IS NULL
            LIMIT ?
            """;
        
        List<SubredditPost> posts = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setInt(1, limit);

            ResultSet result = pstmt.executeQuery();

            while (result.next()) {

                SubredditPost post = new SubredditPost(
                    result.getString("id"), 
                    result.getString("subreddit"), 
                    result.getString("url"), 
                    result.getBoolean("static_downloaded"), 
                    result.getString("screenshot"), 
                    result.getString("json_post"), 
                    result.getTimestamp("inserted_date"),
                    result.getString("static_root_url"),
                    result.getString("static_file_type")
                );

                posts.add(post);
            }

            return posts;


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<String> findAllIds(Connection conn) {
        var postIds = new ArrayList<String>();

        String getAllQuery = "SELECT id FROM subreddit_posts";

        try (var stmt = conn.createStatement()) {

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

    public static List<SubredditPost> getPostsBasedOnStaticFileType(Connection conn, String staticFileType) {
        var queryStaticFileType = """
            SELECT
                id, 
                subreddit, 
                url, 
                static_downloaded, 
                screenshot, 
                json_post, 
                inserted_date, 
                static_root_url,
                static_file_type  
            FROM subreddit_posts 
            WHERE static_downloaded = true
            AND static_file_type = ?
        """;

        try (var pstmt = conn.prepareStatement(queryStaticFileType)) {
            
            pstmt.setString(1, staticFileType);

           ResultSet result = pstmt.executeQuery();
            List<SubredditPost> posts = new ArrayList<>();

            while (result.next()) {
                SubredditPost post = new SubredditPost(
                    result.getString("id"), 
                    result.getString("subreddit"), 
                    result.getString("url"), 
                    result.getBoolean("static_downloaded"), 
                    result.getString("screenshot"), 
                    result.getString("json_post"), 
                    result.getTimestamp("inserted_date"),
                    result.getString("static_root_url"),
                    result.getString("static_file_type")
                );

                posts.add(post);
            }

            return posts;

        } catch (SQLException e) {
            e.printStackTrace();
        }   

        return null;
    }

    public static int updateSubredditPostScreenshot(Connection conn, String id, String screenshotPath) {
        var updatePostQuery = "UPDATE subreddit_posts SET screenshot = ? WHERE id = ?;";

        try (var pstmt = conn.prepareStatement(updatePostQuery)) {
            
            pstmt.setString(1, screenshotPath);
            pstmt.setString(2, id);

            int result = pstmt.executeUpdate();
                
            return result;

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return -1;
    }

    public static int updateSubredditJSON(Connection conn, String id, String jsonPath) {
        var updatePostQuery = "UPDATE subreddit_posts SET json_post = ? WHERE id = ?";

        try (var pstmt = conn.prepareStatement(updatePostQuery)) {

            
            pstmt.setString(1, jsonPath);
            pstmt.setString(2, id);

            int result = pstmt.executeUpdate();

            return result;


        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return -1;
    }

    public static int updateSubredditPostStaticPath(Connection conn, String id, String staticPath) {

        var updateQuery = "UPDATE subreddit_posts SET static_root_url = ? WHERE id = ?";

        try (var pstmt = conn.prepareStatement(updateQuery)) {
            pstmt.setString(1, staticPath);
            pstmt.setString(2, id);

            int result = pstmt.executeUpdate();

            return result;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;

    }

    public static int updateStaticDownloadedFlagTrue(Connection conn, String id) {

        var updateStaticDownloadedFlagQueiry = "UPDATE subreddit_posts SET static_downloaded = ? WHERE id = ?";

        try (var pstmt = conn.prepareStatement(updateStaticDownloadedFlagQueiry)) {
            pstmt.setBoolean(1, true);
            pstmt.setString(2, id);

            int result = pstmt.executeUpdate();

            return result;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;

    }

    public static int updateStaticFields(Connection conn, SubredditPost post) {
                    
        var updateStaticFieldQuery = """
                UPDATE subreddit_posts
                SET static_downloaded = ?,
                static_root_url = ?,
                static_file_type = ?
                WHERE id = ?
                """;

        try (PreparedStatement pstmt = conn.prepareStatement(updateStaticFieldQuery)) {
            pstmt.setBoolean(1, post.isStaticDownloaded());
            pstmt.setString(2, post.getStaticRootPath());
            pstmt.setString(3, post.getStaticFileType());
            pstmt.setString(4, post.getId());

            int result = pstmt.executeUpdate();

            return result;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static int updateStaticFileType(Connection conn, String id, String fileType) {
        var updateQuery = "UPDATE subreddit_posts SET static_file_type = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {
            pstmt.setString(1, fileType);
            pstmt.setString(2, id);

            int result = pstmt.executeUpdate();

            return result;


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;

    }

}
