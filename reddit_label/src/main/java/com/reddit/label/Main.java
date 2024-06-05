package com.reddit.label;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) {

        Path localCsvPath = Paths.get("./");
        //SubredditIngestor.CsvListBuilder(localCsvPath, "https://www.reddit.com/r/CombatFootage/new/");
        SubredditIngestor.LocalDirectoryBuilder(localCsvPath);

    }

 }
    
    
    