module github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api

go 1.20

replace github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer => ../data_layer

require github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer v0.0.0-00010101000000-000000000000

require github.com/neo4j/neo4j-go-driver/v5 v5.24.0 // indirect