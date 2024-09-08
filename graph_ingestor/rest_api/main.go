package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
)

func main() {

	ServeMux := GenerateServerMux()
	err := http.ListenAndServe(":3333", ServeMux)

	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
