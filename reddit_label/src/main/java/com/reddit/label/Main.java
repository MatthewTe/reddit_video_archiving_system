package com.reddit.label;

import org.apache.commons.cli.*;

import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.SubredditIngestor.SubredditPostIngestor;

public class Main {

    public static void main(String[] args) {

        // Command line args:
        Options options = new Options();
        options.addOption("s", "subreddit", true, "The subreddit to extract posts from");
        options.addOption("c", "continue_past_unique", true, "If the ingestor will exit when an article pull extracts no unique posts");
        options.addOption("ct", "create_tables", true, "If subreddit post tables in the database should try to be created");

        String subreddit = "";
        boolean continuePastUnique = false;
        boolean tryCreateTable = false;

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            subreddit = cmd.getOptionValue("subreddit");
            continuePastUnique = cmd.hasOption("continue_past_unique");
        } catch (ParseException e) {
            System.err.println("Error with parsing command lines" + e.getMessage());
            System.exit(1);
        }

        if (tryCreateTable) {
            System.out.println("Trying to create subreddit post database tables...");
            SubredditTablesDB.createSubredditTables();
        }

        System.out.println(String.format(
            "Starting reddit post ingestion for subreddit %s with Continue-Past-Unique set to %b", 
            subreddit,
            continuePastUnique
        ));

        SubredditPostIngestor.RunSubredditIngestor(subreddit, continuePastUnique);
    }

 }
    
    
