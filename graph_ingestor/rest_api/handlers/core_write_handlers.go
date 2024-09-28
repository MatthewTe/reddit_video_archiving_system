package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api/api"
)

func HandleCheckIfNodesExist(w http.ResponseWriter, r *http.Request) {

	var env api.Neo4JEnvironment = api.Neo4JEnvironment{
		URI:      os.Getenv("NEO4J_URI"),
		User:     os.Getenv("NEO4J_USER"),
		Password: os.Getenv("NEO4J_PASSWORD"),
	}
	var ctx context.Context = context.Background()

	rawIds := r.URL.Query()["post_ids"]
	var idsToCheck []string = strings.Split(rawIds[0], ",")

	checkPostExistsResult, err := api.CheckNodeExists(idsToCheck, env, ctx)
	if err != nil {
		http.Error(w, "Error in checking if the post exists: "+err.Error(), http.StatusBadRequest)
	}

	checkPostExistsData, err := json.Marshal(checkPostExistsResult)
	if err != nil {
		http.Error(w, "Error in marshaling JSON data for Post Exists response", http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(checkPostExistsData)

}

func HandleNodeEdgeCreationRequest(w http.ResponseWriter, r *http.Request) {

	buff, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read all bytes from request body", http.StatusBadRequest)
		return
	}

	var env api.Neo4JEnvironment = api.Neo4JEnvironment{
		URI:      os.Getenv("NEO4J_URI"),
		User:     os.Getenv("NEO4J_USER"),
		Password: os.Getenv("NEO4J_PASSWORD"),
	}
	var ctx context.Context = context.Background()

	queryResponseMap, err := api.CoreInsertGraphData(buff, env, ctx)
	if err != nil {
		http.Error(w, "Error in completing the cypher query "+err.Error(), http.StatusBadRequest)
		return
	}

	queryResponseData, err := json.Marshal(queryResponseMap)
	if err != nil {
		http.Error(w, "Error in marshaling the query update response from the Core Insert Graph Data method"+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(queryResponseData)

}
