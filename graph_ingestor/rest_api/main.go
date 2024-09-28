package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
)

func main() {

	args := os.Args
	envFilePath := args[1]

	loadEnvVariablesFromFile(envFilePath)

	router := GenerateServerMux()

	// Adding Middleware:
	middleware := NewLoggerMiddleware(router)

	server := http.Server{
		Addr:    ":8080",
		Handler: middleware,
	}

	err := server.ListenAndServe()

	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
