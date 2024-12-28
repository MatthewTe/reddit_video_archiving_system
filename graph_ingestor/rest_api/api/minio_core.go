package api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioEnvironment struct {
	Endpoint        string
	AccessKeyId     string
	SecretAccessKey string
}

type MinioBlobObject struct {
	FileName     string `json:"file_name"`
	FileSize     int64  `json:"file_size"`
	LastModified string `json:"list_modified"`
}

func ListBlobsFromFilePath(prefixPath string, bucketName string, env MinioEnvironment, ctx context.Context) ([]MinioBlobObject, error) {

	// Create client.
	minioClient, err := minio.New(env.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(env.AccessKeyId, env.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Printf("Error in creating the minio client from env struct")
		return nil, err
	}

	var objectCh <-chan minio.ObjectInfo

	if prefixPath == "" {
		objectCh = minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
			// Prefix:    prefixPath,
			Recursive: false,
		})
	} else {
		objectCh = minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
			Prefix:    prefixPath,
			Recursive: false,
		})
	}

	var minioItems []MinioBlobObject
	for object := range objectCh {
		if object.Err != nil {
			log.Printf("Error in extracting object from prefix: %s. Error %s", prefixPath, object.Err)
		} else {
			minioObject := MinioBlobObject{
				FileName:     object.Key,
				FileSize:     object.Size,
				LastModified: object.LastModified.Format(time.ANSIC),
			}
			minioItems = append(minioItems, minioObject)
		}
	}
	return minioItems, nil
}

func ListBucketAccessPolicy(bucketName string, env MinioEnvironment, ctx context.Context) (string, error) {

	minioClient, err := minio.New(env.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(env.AccessKeyId, env.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Printf("Error in creating the minio client from env struct")
		return "", err
	}

	bucketExists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		log.Printf("Error in trying to see if bucket %s exists", bucketName)
		return "", err
	}

	if bucketExists {
		policy, err := minioClient.GetBucketPolicy(ctx, bucketName)
		if err != nil {
			log.Printf("Error in accessing the access policy of bucket %s", bucketName)
			return "", err
		}

		return policy, nil

	} else {
		return "", fmt.Errorf("bucket %s does not exist", bucketName)
	}

}

func ListBuckets(env MinioEnvironment, ctx context.Context) ([]string, error) {

	minioClient, err := minio.New(env.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(env.AccessKeyId, env.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Printf("Error in creating the minio client from env struct")
		return nil, err
	}

	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
		log.Printf("Error in listing all of the buckets for %s", env.Endpoint)
		return nil, err
	}

	var bucketNames []string
	for _, bucket := range buckets {
		bucketNames = append(bucketNames, bucket.Name)
	}

	return bucketNames, nil

}
