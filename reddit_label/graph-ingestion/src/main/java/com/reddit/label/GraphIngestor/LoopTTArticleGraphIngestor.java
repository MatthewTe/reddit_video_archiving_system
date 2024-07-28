package com.reddit.label.GraphIngestor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Driver;

import com.reddit.label.GraphTypes.LoopTTArticle;

public class LoopTTArticleGraphIngestor {
    public static void MigrateLoopTTArticlesToGraphDB(Connection conn, Driver driver) throws SQLException {

        // Grab all of the ids in the neo4j database.
        List<String> existingArticleNodes = new ArrayList<>();
        var result = driver.executableQuery("MATCH (n:Article:LoopTT) RETURN n.id as id")
            .execute();
        var record = result.records();
        record.forEach(r -> {
            existingArticleNodes.add(r.get("id").asString());
        });

        String queryAllNewArticles;
        if (existingArticleNodes.size() == 0) {
            System.out.println("Zero records found in the graph database. Ingesting all of the articles into the Graph database.");
            queryAllNewArticles = "SELECT * FROM articles";
        } else {
            System.out.printf("Queried %s existing article nodes from the graph database \n", existingArticleNodes.size());

            StringJoiner placeholders = new StringJoiner(", ");
            for (int i = 0; i < existingArticleNodes.size(); i++) {
                placeholders.add("?");
            }

            queryAllNewArticles = String.format("SELECT * FROM articles WHERE id NOT IN (%s)", placeholders.toString());
        }

        // Grab all of the posts from the postgres database that aren't in the existing list.
        try (var pstms = conn.prepareStatement(queryAllNewArticles)) {
            for (int j = 0;  j < existingArticleNodes.size(); j++) {
                pstms.setString(j+1, existingArticleNodes.get(j));
            }
            List<LoopTTArticle> newArticlesToInsert = new ArrayList<>();
            try (ResultSet rs = pstms.executeQuery()) {
                while (rs.next()) {
                    LoopTTArticle newArticle = new LoopTTArticle(
                        rs.getString("id"),
                        rs.getString("title"),
                        rs.getString("url"),
                        rs.getString("type"),
                        rs.getString("content"),
                        rs.getString("source"),
                        rs.getString("extracted_date"), 
                        rs.getString("published_date"), 
                        null,
                        null 
                    );

                    newArticlesToInsert.add(newArticle);
                }

            System.out.printf("%s new articles to insert into the graph database.", newArticlesToInsert.size());
            for (LoopTTArticle article: newArticlesToInsert) {

                // Iterate through all of the articles and insert them into the neo4j Database.
                var insertResult = driver.executableQuery("""
                    CREATE (a:Article:LoopTT {
                        id: $id,
                        title: $title,
                        url: $url,
                        type: $type,
                        content: $content,
                        source: $source,
                        extracted_date: $extracted_date,
                        published_date: $published_date
                    })
                    """)
                .withParameters(Map.of(
                    "id", article.getId(),
                    "title", article.getTitle(),
                    "url", article.getUrl(),
                    "type", article.getType(),
                    "content", article.getContent(),
                    "source", article.getSource(),
                    "extracted_date", article.getExtracted_date(),
                    "published_date", article.getPublished_date()
                    ))
                .execute();

                var summary = insertResult.summary();
                System.out.printf("Created %d records in %d ms.%n",
                    summary.counters().nodesCreated(),
                    summary.resultAvailableAfter(TimeUnit.MILLISECONDS)
                );
                            
            }

            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            
        }

    }
}
