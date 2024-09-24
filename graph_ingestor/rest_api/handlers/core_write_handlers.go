package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api/api"
)

func HandleNodeEdgeCreationRequest(w http.ResponseWriter, r *http.Request) {

	buff, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read all bytes from request body", http.StatusBadRequest)
		return
	}

	var env api.Neo4JEnvironment = api.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
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
