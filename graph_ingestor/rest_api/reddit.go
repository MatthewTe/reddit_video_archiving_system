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
}

func AttachRedditPostStaticFiles(w http.ResponseWriter, r *http.Request) {

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
		http.Error(w, "Error in unmarshaling json request", http.StatusBadRequest)
		return
	}

	AttachedStaticFileResult, err := reddit.AttachRedditPostStaticFiles(redditPost, env, ctx)
	if err != nil {
		http.Error(w, "Error in attaching static files to reddit post: "+err.Error(), http.StatusBadRequest)
		return
	}

	attachedStaticFileResultData, err := json.Marshal(AttachedStaticFileResult)
	if err != nil {
		http.Error(w, "Error in marshaling json values from AttachedStaticFileResult"+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(attachedStaticFileResultData)
}

func InsertRedditUser(w http.ResponseWriter, r *http.Request) {

	var redditUser reddit.RedditUser
	var env config.Neo4JEnvironment = config.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}
	var ctx context.Context = context.Background()

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&redditUser)
	if err != nil {
		http.Error(w, "Error in unmarshaling json request", http.StatusBadRequest)
		return
	}

	InsertRedditUser, err := reddit.AddRedditUser(redditUser, env, ctx)
	if err != nil {
		http.Error(w, "Error in inserting Reddit User into database", http.StatusBadRequest)
		return
	}

	insertedRedditUserData, err := json.Marshal(InsertRedditUser)
	if err != nil {
		http.Error(w, "Error in marshaling json values for Inserting a Reddit User", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(insertedRedditUserData)
}

type AttachedRedditUserRequest struct {
	ParentPost   reddit.RawRedditPostResult `json:"parent_post"`
	AttachedUser reddit.RedditUser          `json:"attached_user"`
}

func AttachRedditUserToPost(w http.ResponseWriter, r *http.Request) {
	var attachedRedditUserRequest AttachedRedditUserRequest
	var env config.Neo4JEnvironment = config.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}
	var ctx context.Context = context.Background()

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&attachedRedditUserRequest)
	if err != nil {
		http.Error(w, "Error in unmarshaling json request", http.StatusBadRequest)
		return
	}

	attachedUserResult, err := reddit.AttachRedditUser(
		attachedRedditUserRequest.ParentPost, attachedRedditUserRequest.AttachedUser, env, ctx)
	if err != nil {
		http.Error(w, "Error in attaching reddit user to post: "+err.Error(), http.StatusBadRequest)
		return
	}

	attachedUserResultData, err := json.Marshal(attachedUserResult)
	if err != nil {
		http.Error(w, "Error in marshaing json values for Attaching Reddit User", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(attachedUserResultData)
}

type AppendRedditPostCommentsRequest struct {
	RedditPost     reddit.RawRedditPostResult
	RedditComments []reddit.RedditComment
}

func AppendRedditComments(w http.ResponseWriter, r *http.Request) {
	var appendRedditPostCommentsInput AppendRedditPostCommentsRequest
	var env config.Neo4JEnvironment = config.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}
	var ctx context.Context = context.Background()

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&appendRedditPostCommentsInput)
	if err != nil {
		http.Error(w, "Error in unmarshaling json request", http.StatusBadRequest)
	}

	redditPostRawResult, RedditComments, err := reddit.ApppendRedditPostComments(
		appendRedditPostCommentsInput.RedditPost,
		appendRedditPostCommentsInput.RedditComments,
		env,
		ctx,
	)
	if err != nil {
		http.Error(w, "Error in inserting and appending reddit post comments "+err.Error(), http.StatusBadRequest)
	}

	var AppendRedditPostCommentsResponse AppendRedditPostCommentsRequest = AppendRedditPostCommentsRequest{
		RedditPost:     redditPostRawResult,
		RedditComments: RedditComments,
	}

	appendRedditPostCommentData, err := json.Marshal(AppendRedditPostCommentsResponse)
	if err != nil {
		http.Error(w, "Error in marshaling JSON data for Appending Reddit Comments "+err.Error(), http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(appendRedditPostCommentData)

}
