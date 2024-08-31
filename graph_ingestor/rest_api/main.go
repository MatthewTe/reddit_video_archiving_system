package main

import (
	"fmt"

	datalayer "github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer"
)

func main() {
	message := datalayer.Hello("Hello World")
	fmt.Println(message)
}
