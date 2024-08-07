package com.reddit.label.Parsers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.reddit.label.RedditContentPostType;
import com.reddit.label.RedditJsonParserResponse;
import com.reddit.label.RedditPostJsonDefaultAttributes;

public class RedditJsonParser {
    
    private static ObjectMapper objectMapper = getDefaultObjectMapper(); 

    private static ObjectMapper getDefaultObjectMapper() {
        ObjectMapper defaultObjectMapper = new ObjectMapper();
        return defaultObjectMapper;
   }

   public static JsonNode parse(String jsonString) throws IOException, JsonParseException {
    return objectMapper.readTree(jsonString);
   }

    public static RedditJsonParserResponse parseDefaultRedditPostJson(String fullJsonString) throws JsonProcessingException, IOException {
        /**
         * Ingests the String representation of the json file assocaited with an individual subreddit post parses it
         * to extract several key fields using Jackson.
         * 
         * Many of the attributes for reddit post's json representation are universial such as the title, subreddit, url and id
         * but the parser also determines what media is present in the reddit post. Eg: Does the post contain a video, images, a gif etc.
         * 
         * The way of determining the media type is unique for each media type and this is the main purpose of this method.
         * 
         * @param fullJsonString The full string representation of the JSON file to be parsed. 
         * 
         * @returns RedditJsonParserResponse The object containing all of of the extracted fields from the parsed Json file.
         */

        RedditPostJsonDefaultAttributes defaultAttributes = new RedditPostJsonDefaultAttributes();

        JsonNode rootNode = objectMapper.readTree(fullJsonString);

        // Default parsing logic that should be consistent with all reddit post json responses:
        if (rootNode.isArray()) {

            System.out.println("Beginning to parse JSON from reddit post");

            ArrayNode rootArrayNode = (ArrayNode)rootNode;
            JsonNode firstArrayItem = rootArrayNode.get(0);
            JsonNode mainAttributesNode = firstArrayItem.get("data").get("children").get(0).get("data");
            
            defaultAttributes.setTitle(mainAttributesNode.get("title").asText());
            defaultAttributes.setSubReddit(mainAttributesNode.get("subreddit").asText());
            defaultAttributes.setUrl(mainAttributesNode.get("url").asText());
            defaultAttributes.setId(mainAttributesNode.get("id").asText());

            // Determining the media associated with the post:
            if (!mainAttributesNode.has("post_hint")) {
                System.out.println("post_hint not found in the subreddit. Setting the attribute to null");
                defaultAttributes.setPostHint(null);
                defaultAttributes.setStaticFileType(RedditContentPostType.NO_STATIC);
            } else {

                String postHint = mainAttributesNode.get("post_hint").asText();
                defaultAttributes.setPostHint(postHint);

                if (postHint.contains("hosted:video")) {
                    defaultAttributes.setStaticFileType(RedditContentPostType.HOSTED_VIDEO);
                } else if (postHint.contains("image")) {
                    defaultAttributes.setStaticFileType(RedditContentPostType.IMAGE);
                }

            }

            return new RedditJsonParserResponse(defaultAttributes, rootNode);
        } 

        System.out.println("No array found for the root node. This implies something incorrect about the schema");
        return null;

   }

}
