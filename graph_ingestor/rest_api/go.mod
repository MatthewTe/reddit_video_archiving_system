module github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api

go 1.22

replace github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer => ../data_layer

require github.com/neo4j/neo4j-go-driver/v5 v5.24.0
