package main

import "net/http"

func GenerateServerMux() *http.ServeMux {

	// ServerMux implements the http.Handler interface:
	router := http.NewServeMux()

	router.HandleFunc("POST /v1/reddit/create_post", HandleInsertRedditPost)
	router.HandleFunc("POST /v1/reddit/attach_static_file", HandleAttachRedditPostStaticFiles)
	router.HandleFunc("POST /v1/reddit/create_user", HandleInsertRedditUser)
	router.HandleFunc("POST /v1/reddit/attach_user_to_post", HandleAttachRedditUserToPost)
	router.HandleFunc("POST /v1/reddit/append_comments", HandleAppendRedditComments)
	router.HandleFunc("GET /v1/reddit/check_posts_exists", HandleCheckRedditPostExists)

	return router
}
