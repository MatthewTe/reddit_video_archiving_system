package com.reddit.label;

public class RedditPostJsonDefaultAttributes {

    String subReddit;
    String title;
    RedditContentPostType staticFileType;
    String url; 
    String postHint;
    String id;

    public RedditPostJsonDefaultAttributes() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "RedditPostJsonDefaultAttributes [title=" + title + ", staticFileType=" + staticFileType + ", url=" + url
                + "]";
    }

    public String getSubReddit() {
        return subReddit;
    }

    public void setSubReddit(String subReddit) {
        this.subReddit = subReddit;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public RedditContentPostType getStaticFileType() {
        return staticFileType;
    }

    public void setStaticFileType(RedditContentPostType staticFileType) {
        this.staticFileType = staticFileType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPostHint() {
        return postHint;
    }

    public void setPostHint(String postHint) {
        this.postHint = postHint;
    }
}
