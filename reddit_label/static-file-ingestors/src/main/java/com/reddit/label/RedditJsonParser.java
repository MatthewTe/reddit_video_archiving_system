package com.reddit.label;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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

            if (!mainAttributesNode.has("post_hint")) {
                System.out.println("post_hint not found in the subreddit. Setting the attribute to null");
                defaultAttributes.setPostHint(null);
            } else {
                defaultAttributes.setPostHint(mainAttributesNode.get("post_hint").asText());
            }


            // TODO: Extract the type of static file associated w/ the post and set is as the enum type

            return new RedditJsonParserResponse(defaultAttributes, rootNode);
        } 

        System.out.println("No array found for the root node. This implies something incorrect about the schema");
        return null;

   }

}
