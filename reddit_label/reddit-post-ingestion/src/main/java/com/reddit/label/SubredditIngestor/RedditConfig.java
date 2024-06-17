package com.reddit.label.SubredditIngestor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RedditConfig {
    private static final Properties properties = new Properties();
    
    static {
        try (InputStream input = RedditConfig.class.getClassLoader().getResourceAsStream("reddit.properties")) {
            if (input == null) {
                System.out.println("Unable to find reddit properties");
                System.exit(1);
            }
            
            properties.load(input);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getRedditUsername() {
        return properties.getProperty("reddit.username");
    }
    
    public static String getRedditPassword() {
        return properties.getProperty("reddit.password");
    }




}
