package com.reddit.label;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class App {
    public static void main( String[] args ) throws IOException
    {

        /*
         * Endpoints that I want:
         * - Trigger the logic that ingests subreddit posts to the postgres database.
         * - Trigger the logic that takes the top 10 non-processed subreddit posts from the database 
         *      and ingests the static content. 
         */
        SpringApplication.run(Main.class, args);


    }
}
