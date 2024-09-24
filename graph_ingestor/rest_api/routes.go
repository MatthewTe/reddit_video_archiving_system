package main

import (
	"net/http"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api/handlers"
)

func GenerateServerMux() *http.ServeMux {

	// ServerMux implements the http.Handler interface:
	router := http.NewServeMux()

	router.HandleFunc("POST /v1/api/run_query", handlers.HandleNodeEdgeCreationRequest)

	return router
}
