package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/config"
	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/reddit"
)

func InsertRedditPost(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "POST":

		var redditPost reddit.RedditPost
		var env config.Neo4JEnvironment = config.Neo4JEnvironment{
			URI:      "neo4j://localhost:7687",
			User:     "neo4j",
			Password: "test_password",
		}
		var ctx context.Context = context.Background()

		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&redditPost)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		fmt.Printf("Recieved Reddit Post: %+v\n", redditPost)

		fmt.Printf("Inserting Reddit Post: %+v into Graph Database", redditPost)
		insertedRedditPostResult, err := reddit.InsertRedditPost(redditPost, env, ctx)
		if err != nil {
			http.Error(w, "Error in creating the reddit post: "+err.Error(), http.StatusBadRequest)
			return
		}

		insertedRedditResponseData, err := json.Marshal(insertedRedditPostResult)
		if err != nil {
			http.Error(w, "Error marshaling json response", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(insertedRedditResponseData)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

}
