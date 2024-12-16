package main

import (
	"log"
	"net/http"
	"time"
)

// Create the struct for the middleware that has the Handler struct.
// Attach a function to the logger Struct that implements the middleware logic:

type LoggerMiddleware struct {
	Handler http.Handler
}

func (l *LoggerMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
