package com.reddit.label.GraphTypes;

import org.neo4j.driver.types.Point;

public class LoopTTArticle {
    public String id; 
    public String title;
    public String url;
    public String type;
    public String content;
    public String source;
    public String extracted_date; 
    public String published_date;
    public Point point;
    public String h3Index;


    public LoopTTArticle(String id, String title, String url, String type, String content, String source,
            String extracted_date, String published_date, Point point, String h3Index) {
        this.id = id;
        this.title = title;
        this.url = url;
        this.type = type;
        this.content = content;
        this.source = source;
        this.extracted_date = extracted_date;
        this.published_date = published_date;
        this.point = point;
        this.h3Index = h3Index;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
    }
    public String getSource() {
        return source;
    }
    public void setSource(String source) {
        this.source = source;
    }
    public String getExtracted_date() {
        return extracted_date;
    }
    public void setExtracted_date(String extracted_date) {
        this.extracted_date = extracted_date;
    }
    public String getPublished_date() {
        return published_date;
    }
    public void setPublished_date(String published_date) {
        this.published_date = published_date;
    }
    public Point getPoint() {
        return point;
    }
    public void setPoint(Point point) {
        this.point = point;
    }
    public String getH3Index() {
        return h3Index;
    }
    public void setH3Index(String h3Index) {
        this.h3Index = h3Index;
    }
    @Override
    public String toString() {
        return "LoopTTArticle [id=" + id + ", title=" + title + ", url=" + url + ", type=" + type + ", content="
                + content + ", source=" + source + ", extracted_date=" + extracted_date + ", published_date="
                + published_date + ", point=" + point + ", h3Index=" + h3Index + "]";
    }

}
