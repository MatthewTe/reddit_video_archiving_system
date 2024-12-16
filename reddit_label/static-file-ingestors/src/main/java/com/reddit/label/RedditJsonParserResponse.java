package com.reddit.label;

import com.fasterxml.jackson.databind.JsonNode;

public class RedditJsonParserResponse {

    RedditPostJsonDefaultAttributes initalPostFields;
    JsonNode fullPostJson;

    public RedditJsonParserResponse(RedditPostJsonDefaultAttributes initalPostFields, JsonNode fullPostJson) {
        this.initalPostFields = initalPostFields;
        this.fullPostJson = fullPostJson;

    }

    @Override
    public String toString() {
        return "RedditJsonParserResponse [initalPostFields=" + initalPostFields + ", fullPostJson=" + fullPostJson
                + "]";
    }

    public RedditPostJsonDefaultAttributes  getInitalPostFields() {
        return initalPostFields;
    }

    public JsonNode getFullPostJson() {
        return fullPostJson;
    }


}
