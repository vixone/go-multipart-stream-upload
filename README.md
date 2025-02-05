# S3 Multipart Upload Lambda (Go)

This Lambda function downloads a large XML feed/or any other file
from a specified URL, streams the file in chunks, and uploads it directly to an S3 bucket using multipart upload. 
The function is implemented in Go and makes use of AWS SDK to interact with S3.

## Features

- **Streams the XML feed** directly from the source without holding it in memory.
- **Uploads the feed in parts** using S3 multipart upload to handle large files efficiently.
- **No memory overhead**: Only one chunk of the file is in memory at a time.
  
## Prerequisites

- **AWS Account**: To deploy the Lambda function and access S3.
- **AWS CLI**: For managing AWS resources locally.
- **Go**: To compile the Lambda function.
- **Terraform**: (TODO: add Terraform configuration to automate deployment)

## Setup

### 1. Install Dependencies

Make sure you have Go installed. If not, install it from the [official site](https://golang.org/dl/).

### 2. Set Up AWS Credentials

Ensure that your AWS credentials are configured, either via the AWS CLI or environment variables.

```bash
aws configure
