package main

import "net/http"

func GenerateServerMux() *http.ServeMux {

	// ServerMux implements the http.Handler interface:
	router := http.NewServeMux()

	router.HandleFunc("POST /v1/reddit/reddit_post", InsertRedditPost)

	return router
}
