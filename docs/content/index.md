+++
title = 'Index'
date = 2024-09-06T22:37:44-07:00
draft = false
+++

## Reddit Data Schema:
### Reddit Post:
```
(post:Reddit:Post:Entity 
    {
        id: $id, 
        url: $url, 
        title: $title, 
        static_root_url: $static_root_url,
        published_date: $published_date
    }
)
```
### Subreddit:
```
(subreddit:Subreddit:Entity {name: $subreddit_name})
```
### Static Files
Reddit specific static file settings:
```
(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag {downloaded: $downloaded_bool})

(static_file_type:StaticFileType:StaticFile:Settings {type: $static_file_type})
```

Actual static file content:
```
(screenshot:Reddit:Screenshot:StaticFile:Image {path: $screenshot_path})

(json:Reddit:Json:StaticFile {path: $json_path})

(video:Reddit:StaticFile:Video {path: $mpd_filepath})
```

### Users:
```
(user:Reddit:User:Entity:Account 
    {
        name: $author_name,
        full_name: $author_full_name
    }
)
```

### Comments:
```
(comment:Reddit:Entity:Comment 
    {
        reddit_post_id: $reddit_post_id,
        id: $comment_id,
        body: $comment_body
    }
)
```