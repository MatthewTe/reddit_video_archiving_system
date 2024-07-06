package com.reddit.label.GraphIngestor;

public class RedditPostGraphIngestionResponse {
    private String redditPostId;
    private String videoFileName;
    private String screenshotFileName;
    private String jsonFileName;

    public RedditPostGraphIngestionResponse(String redditPostId, String videoNodeFileName, String screenshotFileName,
            String jsonFileName) {
        this.redditPostId = redditPostId;
        this.videoFileName = videoNodeFileName;
        this.screenshotFileName = screenshotFileName;
        this.jsonFileName = jsonFileName;
    }

    public String getRedditPostId() {
        return redditPostId;
    }

    public String getVideoFileName() {
        return videoFileName;
    }

    public String getScreenshotFileName() {
        return screenshotFileName;
    }

    public String getJsonFileName() {
        return jsonFileName;
    }

}
