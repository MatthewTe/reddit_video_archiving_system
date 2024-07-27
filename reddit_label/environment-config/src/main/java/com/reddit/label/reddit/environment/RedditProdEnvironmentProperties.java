package com.reddit.label.reddit.environment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class RedditProdEnvironmentProperties implements RedditEnvironment {
    
    public void loadEnvironmentFromFile(String pathToFile) throws IOException {

        File file = new File(pathToFile);
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String envLine;

        while ((envLine = reader.readLine()) != null) {

            if (envLine.contains("=")) {
                String[] envLineParts = envLine.split("=");
                String name = String.format("reddit.%s", envLineParts[0]);
                String value = envLineParts[1];

                if ((System.getenv(name) != null)) {
                    System.out.printf("Environment variable %s already exists....Overwriting it \n", name);
                } else {
                    System.out.printf("Environment variable %s does not exist....Overwriting it \n", name);
                }
                System.setProperty(name, value);
            } else { 
                continue;
            }
        }

        reader.close();       

    }

    public String getUsername() {
        return System.getProperty("reddit.REDDIT_PROD_USERNAME");
    }

    public String getPassword() {
        return System.getProperty("reddit.REDDIT_PROD_PASSWORD");
    }


}
