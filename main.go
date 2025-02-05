package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Client       *s3.S3
	bucketName     = "your-s3-bucket-name"
	fileURL        = "https://your-xml-feed-url.com/large-xml-file.xml"
	s3Key          = "path/in/s3/large-xml-file.xml"
	uploadPartSize = int64(5 * 1024 * 1024) // Set part size to 5MB
	numWorkers     = 5                      // Number of concurrent worker goroutines
)

func init() {
	// Initialize the AWS session and S3 client
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // Adjust region as needed
	}))
	s3Client = s3.New(sess)
}

func handleRequest(ctx context.Context) error {
	// Create a multipart upload to S3
	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	}

	uploadResp, err := s3Client.CreateMultipartUpload(createMultipartUploadInput)
	if err != nil {
		log.Fatalf("Unable to start multipart upload: %v", err)
		return err
	}
	uploadID := *uploadResp.UploadId
	log.Printf("Started multipart upload with UploadId: %s", uploadID)

	// Download the XML feed as a stream
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Fatalf("Failed to download file: %v", err)
		return err
	}
	defer resp.Body.Close()

	// Channel for uploading parts
	partCh := make(chan []byte, numWorkers)
	// Channel to signal completed parts and collect ETags
	completedPartsCh := make(chan *s3.CompletedPart, numWorkers)

	var wg sync.WaitGroup
	var partNumber int64 = 1

	// Worker function that uploads parts concurrently
	worker := func() {
		for chunk := range partCh {
			uploadPartInput := &s3.UploadPartInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(s3Key),
				PartNumber: aws.Int64(partNumber),
				UploadId:   aws.String(uploadID),
				Body:       bytes.NewReader(chunk),
			}

			uploadPartResp, err := s3Client.UploadPart(uploadPartInput)
			if err != nil {
				log.Printf("Error uploading part %d: %v", partNumber, err)
				return
			}

			log.Printf("Uploaded part %d with ETag: %s", partNumber, *uploadPartResp.ETag)
			completedPartsCh <- &s3.CompletedPart{
				ETag:       uploadPartResp.ETag,
				PartNumber: aws.Int64(partNumber),
			}

			partNumber++
		}
		wg.Done()
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Reading the XML file in parts and sending them to workers
	buf := make([]byte, uploadPartSize)
	partIndex := int64(1)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// Send the chunk to the worker
			partCh <- buf[:n]
			partIndex++
		}
		if err == io.EOF {
			// All parts have been read
			close(partCh)
			break
		}
		if err != nil {
			log.Printf("Error reading file stream: %v", err)
			return err
		}
	}

	// Wait for all workers to finish uploading
	wg.Wait()

	// Collect completed parts' ETags
	var partETags []*s3.CompletedPart
	for i := 0; i < int(partIndex-1); i++ {
		partETags = append(partETags, <-completedPartsCh)
	}

	// Complete the multipart upload
	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(s3Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: partETags,
		},
	}

	_, err = s3Client.CompleteMultipartUpload(completeMultipartUploadInput)
	if err != nil {
		log.Printf("Failed to complete multipart upload: %v", err)
		return err
	}

	log.Println("Multipart upload completed successfully")
	return nil
}

func main() {
	lambda.Start(handleRequest)
}

