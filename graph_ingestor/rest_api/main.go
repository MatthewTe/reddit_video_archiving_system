package main

import (
	"context"
	"time"

	datalayer "github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer"
)

func main() {

	testRedditPost := datalayer.RedditPost{
		Id:               "test_id",
		Subreddit:        "test_subreddit",
		Url:              "test_url",
		Title:            "test_tile",
		StaticDownloaded: false,
		Screenshot:       "test_screenshot_path",
		JsonPost:         "test_json_path",
		CreatedDate:      time.Now(),
		StaticRootUrl:    "example_static_root_path",
		StaticFileType:   "hosted:video",
	}

	testEnvironment := datalayer.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}

	ctx := context.Background()
	datalayer.InsertRedditPost(testRedditPost, testEnvironment, ctx)

}
