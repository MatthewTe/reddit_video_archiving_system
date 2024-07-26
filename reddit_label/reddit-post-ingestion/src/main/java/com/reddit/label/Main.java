package com.reddit.label;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.cli.*;

import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.SubredditIngestor.SubredditPostIngestor;
import com.reddit.label.postgres.connections.PostgresConnector;
import com.reddit.label.postgres.environments.PostgresProdEnvironmentProperties;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        // Command line args:
        Options options = new Options();
        options.addOption("s", "subreddit", true, "The subreddit to extract posts from");
        options.addOption("e", "env-file", true, "Pointing to a path to a production environment file that will be used to run the ingestor");
        options.addOption("c", "continue_past_unique", false, "If the ingestor will exit when an article pull extracts no unique posts");
        options.addOption("ct", "create_tables", false, "If subreddit post tables in the database should try to be created");
        options.addOption("ov", "override_param", true, "If the ingestor needs to start at a specific url point with query params we can use this variable");
        
        String subreddit = "";
        String envFilePath = "";
        String urlOverride = null;
        boolean continuePastUnique = false;
        boolean tryCreateTable = true;

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            subreddit = cmd.getOptionValue("subreddit");
            envFilePath = cmd.getOptionValue("env-file");
            continuePastUnique = cmd.hasOption("continue_past_unique");
            tryCreateTable = cmd.hasOption("create_table");
            urlOverride = cmd.getOptionValue("override_param");
        } catch (ParseException e) {
            System.err.println("Error with parsing command lines" + e.getMessage());
            System.exit(1);
        }

        PostgresProdEnvironmentProperties postgresEnvironment = new PostgresProdEnvironmentProperties();
        postgresEnvironment.loadEnvironmentVariablesFromFile(envFilePath);
        PostgresConnector psqlConnector = new PostgresConnector();
        psqlConnector.loadEnvironment(postgresEnvironment);

        tryCreateTable = true;
        if (tryCreateTable) {
            System.out.println("Trying to create subreddit post database tables...");

            try (Connection conn = psqlConnector.getConnection()) {
                SubredditTablesDB.createSubredditTables(conn);
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }
        }

        System.out.println(String.format(
            "Starting reddit post ingestion for subreddit %s with Continue-Past-Unique set to %b", 
            subreddit,
            continuePastUnique
        ));
        
        if (urlOverride != null) {
            String.format("Manual Override provided $s. Activating Ingestor.", urlOverride);

            try (Connection conn = psqlConnector.getConnection()) {
                SubredditPostIngestor.RunSubredditIngestorOld(conn, subreddit, continuePastUnique, urlOverride);
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

        } else {

            try (Connection conn = psqlConnector.getConnection()) {
                SubredditPostIngestor.RunSubredditIngestorOld(conn, subreddit, continuePastUnique);
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

        }
    }

 }
    
    
