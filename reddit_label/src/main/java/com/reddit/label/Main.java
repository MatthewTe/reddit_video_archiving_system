package com.reddit.label;

import org.apache.commons.cli.*;

import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.SubredditIngestor.SubredditPostIngestor;

public class Main {

    public static void main(String[] args) {

        // Command line args:
        Options options = new Options();
        options.addOption("s", "subreddit", true, "The subreddit to extract posts from");
        options.addOption("c", "continue_past_unique", false, "If the ingestor will exit when an article pull extracts no unique posts");
        options.addOption("ct", "create_tables", false, "If subreddit post tables in the database should try to be created");
        options.addOption("ov", "override_param", true, "If the ingestor needs to start at a specific url point with query params we can use this variable");
        
        String subreddit = "";
        String urlOverride = null;
        boolean continuePastUnique = false;
        boolean tryCreateTable = true;

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            subreddit = cmd.getOptionValue("subreddit");
            continuePastUnique = cmd.hasOption("continue_past_unique");
            tryCreateTable = cmd.hasOption("create_table");
            urlOverride = cmd.getOptionValue("override_param");
        } catch (ParseException e) {
            System.err.println("Error with parsing command lines" + e.getMessage());
            System.exit(1);
        }

        tryCreateTable = true;
        if (tryCreateTable) {
            System.out.println("Trying to create subreddit post database tables...");
            SubredditTablesDB.createSubredditTables();
        }

        System.out.println(String.format(
            "Starting reddit post ingestion for subreddit %s with Continue-Past-Unique set to %b", 
            subreddit,
            continuePastUnique
        ));
        
        if (urlOverride != null) {
            String.format("Manual Override provided $s. Activating Ingestor.", urlOverride);
            SubredditPostIngestor.RunSubredditIngestorOld(subreddit, continuePastUnique, urlOverride);
        } else {
            SubredditPostIngestor.RunSubredditIngestorOld(subreddit, continuePastUnique);
        }
    }

 }
    
    
