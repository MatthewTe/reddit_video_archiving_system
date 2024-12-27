module github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/rest_api

go 1.22

replace github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer => ../data_layer

require github.com/neo4j/neo4j-go-driver/v5 v5.24.0

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.82 // indirect
	github.com/rs/xid v1.6.0 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
)
