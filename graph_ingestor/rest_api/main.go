package main

import (
	"context"
	"fmt"
	"time"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/config"
	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/reddit"
)

func main() {

	testRedditPost := reddit.RedditPost{
		Id:               "test_id_two",
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

	testEnvironment := config.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}

	ctx := context.Background()

	insertedRedditResponse, err := reddit.InsertRedditPost(testRedditPost, testEnvironment, ctx)
	if err != nil {
		fmt.Println(err)
	}

	staticPostFileResult, err := reddit.AppendRawRedditPostStaticFiles(testRedditPost, testEnvironment, ctx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(insertedRedditResponse)
	fmt.Println(staticPostFileResult)

}
