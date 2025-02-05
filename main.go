package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Uploader struct {
	s3Client       *s3.S3
	bucketName     string
	fileURL        string
	s3Key          string
	uploadPartSize int64
	numWorkers     int
}

// NewS3Uploader initializes and returns a new S3Uploader instance.
func NewS3Uploader(bucketName, fileURL, s3Key string, uploadPartSize int64, numWorkers int) *S3Uploader {
	cfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		return nil, err, fmt.Errorf("unable to load AWS config, %v", err)
	}
	return &S3Uploader{
		s3Client:       s3.NewFromConfig(cfg),
		bucketName:     bucketName,
		fileURL:        fileURL,
		s3Key:          s3Key,
		uploadPartSize: uploadPartSize,
		numWorkers:     numWorkers,
	}
}

func (u *S3Uploader) StartMultipartUpload(ctx context.Context) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucketName),
		Key:    aws.String(u.s3Key),
	}

	resp, err := u.s3Client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("unable to start multipart upload: %v", err)
	}

	fmt.Printf("Started multipart upload with UploadId: %s", *resp.UploadId)
	return *resp.UploadId, nil
}
func (u *S3Uploader) DownloadFile() (io.ReadCloser, error) {
	resp, err := http.Get(u.fileURL)
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %v", err)
	}
	return resp.Body, nil
}

func (u *S3Uploader) UploadPart(ctx context.Context, uploadID string, partNumber int32, body []byte) (*s3.CompletedPart, error) {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(u.bucketName),
		Key:        aws.String(u.s3Key),
		PartNumber: aws.Int32(partNumber),
		UploadId:   aws.String(uploadID),
		Body:       bytes.NewReader(body),
	}

	resp, err := u.s3Client.UploadPart(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error uploading part %d: %v", partNumber, err)
	}

	fmt.Printf("Uploaded part %d with ETag: %s", partNumber, *resp.ETag)
	return &s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: aws.Int32(partNumber),
	}, nil
}

func (u *S3Uploader) CompleteMultipartUpload(ctx context.Context, uploadID string, parts []s3.CompletedPart) error {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucketName),
		Key:      aws.String(u.s3Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}

	_, err := u.s3Client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %v", err)
	}

	fmt.Println("Multipart upload completed successfully")
	return nil
}

func (u *S3Uploader) UploadFile(ctx context.Context) error {
	uploadID, err := u.StartMultipartUpload(ctx)
	if err != nil {
		return err
	}

	// start download stream
	fileStream, err := u.DownloadFile()
	if err != nil {
		return err
	}
	defer fileStream.Close()

	buf := make([]byte, u.uploadPartSize)
	var partNumber int32 = 1
	var partETags []s3.CompletedPart

	for {
		n, err := fileStream.Read(buf)
		if n > 0 {
			part, err := u.UploadPart(ctx, uploadID, partNumber, buf[:n])
			if err != nil {
				return err
			}
			partETags = append(partETags, *part)
			partNumber++
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file stream: %v", err)
		}
	}

	return u.CompleteMultipartUpload(ctx, uploadID, partETags)
}

func LambdaHandler(ctx context.Context) error {
	bucketName := os.Getenv("S3_BUCKET_NAME")
	fileUrl := os.Getenv("XML_FILE_URL")
	S3FilePath := "feeds/file.xml"     // make filename unique
	partSize := int64(5 * 1024 * 1024) // 5MB
	numWorkers := 5                    // maybe get it also from env

	uploader := NewS3Uploader(
		bucketName,
		fileUrl,
		S3FilePath,
		partSize,
		numWorkers,
	)

	return uploader.UploadFile(ctx)
}

func main() {
	lambda.Start(LambdaHandler)
}

