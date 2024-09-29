# S3 Receiver

The S3 Receiver is a service for collecting and storing PostgreSQL metric data in S3 storage. We create a new bucket for each database and store measurements as blobs.

*Please set the following environment variables before using this*
 - awsuser: Your AWS username
 - awspasswd: Your AWS Password

## Usage
```bash
go run ./cmd/s3_receiver --port=<port_number_for_sink> --awsEndpoint=<endpoint> --awsRegion=<region(default=us-east-1)>
```