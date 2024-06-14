package com.reddit.label.Databases;

public class SubredditPost {

    // Core fields:
    private String id;
    private String subreddit;
    private String url;
    private boolean staticDownloaded;

    // Additional Fields:
    private String screenshotPath;
    private String jsonPostPath;
    private String staticRootPath;

    public SubredditPost(String id, String subreddit, String url, boolean staticDownloaded, String screenshotPath,
            String jsonPostPath, String staticRootPath) {
        this.id = id;
        this.subreddit = subreddit;
        this.url = url;
        this.staticDownloaded = staticDownloaded;
        this.screenshotPath = screenshotPath;
        this.jsonPostPath = jsonPostPath;
        this.staticRootPath = staticRootPath;
    }

    public SubredditPost(String id, String subreddit, String url, boolean staticDownloaded) {
        this.id = id;
        this.subreddit = subreddit;
        this.url = url;
        this.staticDownloaded = staticDownloaded;
    }

    public SubredditPost(String id, String screenshotPath, String jsonPostPath, String staticRootPath) {
        this.id = id;
        this.screenshotPath = screenshotPath;
        this.jsonPostPath = jsonPostPath;
        this.staticRootPath = staticRootPath;
    }

    public String getScreenshotPath() {
        return screenshotPath;
    }

    public void setScreenshotPath(String screenshotPath) {
        this.screenshotPath = screenshotPath;
    }

    public String getJsonPostPath() {
        return jsonPostPath;
    }

    public void setJsonPostPath(String jsonPostPath) {
        this.jsonPostPath = jsonPostPath;
    }

    public String getStaticRootPath() {
        return staticRootPath;
    }

    public void setStaticRootPath(String staticRootPath) {
        this.staticRootPath = staticRootPath;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSubreddit() {
        return subreddit;
    }

    public void setSubreddit(String subreddit) {
        this.subreddit = subreddit;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isStaticDownloaded() {
        return staticDownloaded;
    }

    public void setStaticDownloaded(boolean staticDownloaded) {
        this.staticDownloaded = staticDownloaded;
    }

    @Override
    public String toString() {
        return "SubredditPost [id=" + id + ", subreddit=" + subreddit + ", url=" + url + ", staticDownloaded="
                + staticDownloaded + "]";
    }
}
