package com.reddit.label.GraphIngestor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import com.reddit.label.neo4j.connections.Neo4jConnector;
import com.reddit.label.neo4j.environments.Neo4jTestEnvironmentProperties;
import com.reddit.label.postgres.connections.PostgresConnector;
import com.reddit.label.postgres.environments.PostgresTestEnvironmentProperties;
import com.reddit.label.GraphTypes.LoopTTArticle;

public class LoopTTArticleGraphIngestorTest {

    Connection conn;
    Driver driver;

    @BeforeEach
    void setUp() throws IOException, SQLException {

        PostgresTestEnvironmentProperties postgresEnvironment = new PostgresTestEnvironmentProperties();
        postgresEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        PostgresConnector postgresConnector = new PostgresConnector();
        postgresConnector.loadEnvironment(postgresEnvironment);

        conn = postgresConnector.getConnection();
        conn.setAutoCommit(false);

        Neo4jTestEnvironmentProperties neo4jEnvironment = new Neo4jTestEnvironmentProperties();
        neo4jEnvironment.loadEnvironmentVariablesFromFile("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/reddit_label/environment-config/src/main/resources/test_dev.env");
        Neo4jConnector neo4jConnector = new Neo4jConnector();
        neo4jConnector.loadEnvironment(neo4jEnvironment);

        driver = neo4jConnector.getDriver();
        
        String createTableQuery = """
            CREATE TABLE IF NOT EXISTS articles (
                id VARCHAR(255),
                title VARCHAR(255),
                url VARCHAR(255),
                type VARCHAR(255),
                content VARCHAR(255),
                source VARCHAR(255),
                extracted_date VARCHAR(255),
                published_date VARCHAR(255)
            )
        """;

        try (var pstmt = conn.prepareStatement(createTableQuery)) {
            pstmt.execute();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (PreparedStatement pstmt = conn.prepareStatement("TRUNCATE TABLE articles")) {
            pstmt.executeUpdate();
        }
    }   

    @AfterEach
    void tearDown() throws SQLException {

        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
            System.out.println("Clear the neo4j database");
        } finally {
            driver.close();
        }

        conn.rollback();
        conn.close();
    }

    @Test
    void testMigrateLoopTTArticlesToGraphDB() throws SQLException {

        String insert_query = """
            INSERT INTO articles(
                id,
                title,
                url,
                type,
                content,
                source,
                extracted_date,
                published_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (var pstmt = conn.prepareStatement(insert_query, Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, "8e682c96-2e23-5b45-b050-04834625ad70");
            pstmt.setString(2, "Man robbed after going to see woman he met on Facebook");
            pstmt.setString(3, "/content/man-robbed-after-going-see-woman-he-met-facebook");
            pstmt.setString(4, "crime");
            pstmt.setString(5, "This is an example content");
            pstmt.setString(6, "https://tt.loopnews.com");
            pstmt.setString(7, "March 23, 2024 01:22 PM");
            pstmt.setString(8, "March 22, 2024 12:18 PM");
            pstmt.addBatch();

            pstmt.setString(1, "5ab08f26-bc61-5dcd-8ffb-1bf751735899");
            pstmt.setString(2, "Two arrested after shooting at woman");
            pstmt.setString(3, "/content/two-arrested-after-shooting-woman");
            pstmt.setString(4, "crime");
            pstmt.setString(5, "This is an example content part 2");
            pstmt.setString(6, "https://tt.loopnews.com");
            pstmt.setString(7, "March 23, 2024 01:22 PM");
            pstmt.setString(8, "March 18, 2024 01:08 PM");
            pstmt.addBatch();

            @SuppressWarnings("unused")
            int[] updateCounts = pstmt.executeBatch();
            conn.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

        LoopTTArticleGraphIngestor.MigrateLoopTTArticlesToGraphDB(conn, driver);

        var result = driver.executableQuery("""
            MATCH (n:Article:LoopTT) RETURN 
            n.id, n.title, n.url, n.type, n.content, n.source, n.extrcted_date, n.published_date
        """)
        .execute();

        List<LoopTTArticle> insertedArticleNodes = new ArrayList<>();
        var record = result.records();
        record.forEach(r -> {

            LoopTTArticle newNode = new LoopTTArticle(
                r.get("id").asString(), 
                r.get("title").asString(), 
                r.get("url").asString(), 
                r.get("type").asString(), 
                r.get("content").asString(), 
                r.get("source").asString(), 
                r.get("extracted_date").asString(), 
                r.get("published-date").asString(), 
                null, 
                null
            );
            insertedArticleNodes.add(newNode);

            System.out.println(r.get("id").asString());
        });

        var insertedNodesResult = driver.executableQuery("MATCH (n:Article:LoopTT) RETURN n").execute();
        var records = insertedNodesResult.records();
        assertEquals(2, records.size());

    }

    @Test
    void testIngestingExistingArticlesFromDb() throws SQLException {
        String insert_query = """
            INSERT INTO articles(
                id,
                title,
                url,
                type,
                content,
                source,
                extracted_date,
                published_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (var pstmt = conn.prepareStatement(insert_query, Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, "8e682c96-2e23-5b45-b050-04834625ad70");
            pstmt.setString(2, "Man robbed after going to see woman he met on Facebook");
            pstmt.setString(3, "/content/man-robbed-after-going-see-woman-he-met-facebook");
            pstmt.setString(4, "crime");
            pstmt.setString(5, "This is an example content");
            pstmt.setString(6, "https://tt.loopnews.com");
            pstmt.setString(7, "March 23, 2024 01:22 PM");
            pstmt.setString(8, "March 22, 2024 12:18 PM");
            pstmt.addBatch();

            pstmt.setString(1, "5ab08f26-bc61-5dcd-8ffb-1bf751735899");
            pstmt.setString(2, "Two arrested after shooting at woman");
            pstmt.setString(3, "/content/two-arrested-after-shooting-woman");
            pstmt.setString(4, "crime");
            pstmt.setString(5, "This is an example content part 2");
            pstmt.setString(6, "https://tt.loopnews.com");
            pstmt.setString(7, "March 23, 2024 01:22 PM");
            pstmt.setString(8, "March 18, 2024 01:08 PM");
            pstmt.addBatch();

            @SuppressWarnings("unused")
            int[] updateCounts = pstmt.executeBatch();
            conn.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

        LoopTTArticleGraphIngestor.MigrateLoopTTArticlesToGraphDB(conn, driver);

        var result = driver.executableQuery("""
            MATCH (n:Article:LoopTT) RETURN 
            n.id, n.title, n.url, n.type, n.content, n.source, n.extrcted_date, n.published_date
        """)
        .execute();

        List<LoopTTArticle> insertedArticleNodes = new ArrayList<>();
        var record = result.records();
        record.forEach(r -> {

            LoopTTArticle newNode = new LoopTTArticle(
                r.get("id").asString(), 
                r.get("title").asString(), 
                r.get("url").asString(), 
                r.get("type").asString(), 
                r.get("content").asString(), 
                r.get("source").asString(), 
                r.get("extracted_date").asString(), 
                r.get("published-date").asString(), 
                null, 
                null
            );
            insertedArticleNodes.add(newNode);

            System.out.println(r.get("id").asString());
        });

        var insertedNodesResult = driver.executableQuery("MATCH (n:Article:LoopTT) RETURN n").execute();
        var records = insertedNodesResult.records();
        assertEquals(2, records.size());

        // Migrating a new article from the database with the original 2 articles are still in the database:
        try (var pstmt = conn.prepareStatement(insert_query, Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, "66dd084e-9a74-59f6-9d4b-c247d19804a8");
            pstmt.setString(2, "The non-duplicated post");
            pstmt.setString(3, "example_content_param");
            pstmt.setString(4, "crime");
            pstmt.setString(5, "This is an example content");
            pstmt.setString(6, "https://tt.loopnews.com");
            pstmt.setString(7, "March 23, 2024 01:23 PM");
            pstmt.setString(8, "March 22, 2024 12:19 PM");

            pstmt.executeUpdate();

        }


        // Should only ingest the new article: 
        LoopTTArticleGraphIngestor.MigrateLoopTTArticlesToGraphDB(conn, driver);
        var thirdArticleInsertionResult = driver.executableQuery("MATCH (n:Article:LoopTT) RETURN n").execute();
        var thirdArticleInsertionrecords = thirdArticleInsertionResult.records();
        assertEquals(3, thirdArticleInsertionrecords.size());

        // Should not ingest anything:
        LoopTTArticleGraphIngestor.MigrateLoopTTArticlesToGraphDB(conn, driver);
        var noArticleInsertionResult = driver.executableQuery("MATCH (n:Article:LoopTT) RETURN n").execute();
        var noArticleInsertionrecords = noArticleInsertionResult.records();
        assertEquals(3, noArticleInsertionrecords.size());


        

    }

}
