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

	var env api.MinioEnvironment = api.MinioEnvironment{
		Endpoint:        os.Getenv("SERVER_MINIO_ENDPOINT"),
		AccessKeyId:     os.Getenv("SERVER_MINIO_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("SERVER_MINIO_SECRET_KEY"),
	}

	var ctx context.Context = context.Background()

	bucketName := r.URL.Query()["bucket"]
	accessPolicy, err := api.ListBucketAccessPolicy(bucketName[0], env, ctx)
	if err != nil {
		http.Error(w, "Error in extracting the access policy from the bucket. Error: "+err.Error(), http.StatusBadRequest)
	}

	accessPolicyMap := make(map[string]string)
	accessPolicyMap[bucketName[0]] = accessPolicy

	accessPolicyResponse, err := json.Marshal(accessPolicyMap)
	if err != nil {
		http.Error(w, "Error in searlizing the access policy response. Error: "+err.Error(), http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(accessPolicyResponse)

}

func HandleGetAllObjectInBucket(w http.ResponseWriter, r *http.Request) {
	var env api.MinioEnvironment = api.MinioEnvironment{
		Endpoint:        os.Getenv("SERVER_MINIO_ENDPOINT"),
		AccessKeyId:     os.Getenv("SERVER_MINIO_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("SERVER_MINIO_SECRET_KEY"),
	}

	var ctx context.Context = context.Background()
	prefixPath := r.URL.Query()["prefix"]
	bucketName := r.URL.Query()["bucket_name"]

	allBucketObjects, err := api.ListBlobsFromFilePath(prefixPath[0], bucketName[0], env, ctx)
	if err != nil {
		http.Error(w, "Error in extracting all of the objects from the provide bucket. Error: "+err.Error(), http.StatusBadRequest)
	}

	allBucketObjectsResponse, err := json.Marshal(allBucketObjects)
	if err != nil {
		http.Error(w, "Error in seralizing the bucket object extraction response. Error: "+err.Error(), http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(allBucketObjectsResponse)

}
