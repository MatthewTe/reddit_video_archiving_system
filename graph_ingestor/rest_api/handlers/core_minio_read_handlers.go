package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api/api"
)

func HandleListMinioBuckets(w http.ResponseWriter, r *http.Request) {
	var env api.MinioEnvironment = api.MinioEnvironment{
		Endpoint:        os.Getenv("SERVER_MINIO_ENDPOINT"),
		AccessKeyId:     os.Getenv("SERVER_MINIO_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("SERVER_MINIO_SECRET_KEY"),
	}

	var ctx context.Context = context.Background()
	buckets, err := api.ListBuckets(env, ctx)
	if err != nil {
		http.Error(w, "Error in listing the buckets from minio client: "+err.Error(), http.StatusBadRequest)
	}

	bucektsResponse, err := json.Marshal(buckets)
	if err != nil {
		http.Error(w, "Error in marshalling the json response for a list of buckets: "+err.Error(), http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(bucektsResponse)
}

func HandleGetBucketAccessPolicy(w http.ResponseWriter, r *http.Request) {
}

func HandleGetAllObjectInBucket(w http.ResponseWriter, r *http.Request) {

}
