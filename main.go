package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	client     *s3.Client
	bucketName string
}

func NewS3Client() (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		return nil, err, fmt.Errorf("Unable to load AWS config %v", err)
	}

	bucketName := os.Getenv("S3_BUCKET_NAME")

	return &S3Client{
		client:     s3.NewFromConfig(cfg),
		bucketName: bucketName,
	}, nil
}

func LambdaHandler(ctx context.Context) error {

	// create s3 client
	s3Client, err := NewS3Client()

	if err != nil {
		return fmt.Errorf("Error creating s3 client %v", err)
	}

	// download xml feed

	// start multipart upload

	// use workers to upload parts

	fmt.Println("File uploaded to S3 bucket successfully!")
	return nil
}

func main() {
	lambda.Start(LambdaHandler)
}

