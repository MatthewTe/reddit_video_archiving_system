package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// Create the struct for the middleware that has the Handler struct.
// Attach a function to the logger Struct that implements the middleware logic:

type LoggerMiddleware struct {
	Handler http.Handler
}

func (l *LoggerMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if os.Getenv("SERVER_ENV") == "dev" {
		fmt.Printf("Server dev set to %s. Adding CORS enabling to the middleware\n", os.Getenv("SERVER_ENV"))
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	}

	start := time.Now()
	l.Handler.ServeHTTP(w, r)
	log.Printf("%s %s: Time taken - %v", r.Method, r.URL.Path, time.Since(start))

}

// The Constructor
func NewLoggerMiddleware(handlerToWrap http.Handler) *LoggerMiddleware {
	return &LoggerMiddleware{
		Handler: handlerToWrap,
	}
}
