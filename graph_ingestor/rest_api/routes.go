package main

import "net/http"

func GenerateServerMux() *http.ServeMux {

	mux := http.NewServeMux()

	mux.HandleFunc("/v1/reddit/reddit_post", InsertRedditPost)

	return mux
}
