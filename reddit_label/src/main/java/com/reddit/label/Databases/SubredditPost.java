package com.reddit.label.Databases;

public class SubredditPost {
    private String id;
    private String subreddit;
    private String url;
    private boolean staticDownloaded;

    public SubredditPost(String id, String subreddit, String url, boolean staticDownloaded) {
        this.id = id;
        this.subreddit = subreddit;
        this.url = url;
        this.staticDownloaded = staticDownloaded;
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
