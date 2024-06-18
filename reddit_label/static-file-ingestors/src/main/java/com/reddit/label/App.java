package com.reddit.label;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class App 
{
    public static void main( String[] args ) throws IOException
    {

        String jsonData = Files.readString(Paths.get(""));
        
        RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(jsonData);
        
        System.out.println(parsedJsonResponse.getInitalPostFields().getId());
        System.out.println(parsedJsonResponse.getInitalPostFields().getTitle());
        System.out.println(parsedJsonResponse.getInitalPostFields().getSubReddit());
        System.out.println(parsedJsonResponse.getInitalPostFields().getUrl());
        System.out.println(parsedJsonResponse.getInitalPostFields().getPostHint());
        
    
    }
}
