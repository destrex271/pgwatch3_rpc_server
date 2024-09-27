package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3Receiver struct {
	S3Client  *s3.Client
	S3Manager *manager.Uploader
	Ctx       context.Context
	sinks.SyncMetricHandler
}

func NewS3Receiver(awsEndpoint string, awsRegion string) (*S3Receiver, error) {
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}

	// Create the resource client
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(awsEndpoint)
	})

	recv := &S3Receiver{
		S3Client:          client,
		Ctx:               context.Background(),
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return recv, nil
}

func (r *S3Receiver) AddDatabase(dbname string) error {
	// Each Bucket stores all metrics for one database
	if _, err := r.S3Client.CreateBucket(r.Ctx, &s3.CreateBucketInput{
		Bucket: &dbname,
	}); err != nil {
		return err
	}

	return nil
}

func (r *S3Receiver) DBExists(bucketName string) (bool, error) {
	_, err := r.S3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	exists := true
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				log.Printf("Bucket %v is available.\n", bucketName)
				exists = false
				err = nil
			default:
				log.Printf("Either you don't have access to bucket %v or another error occurred. "+
					"Here's what happened: %v\n", bucketName, err)
			}
		}
	} else {
		log.Printf("Bucket %v exists and you already own it.", bucketName)
	}

	return exists, err
}

func (r *S3Receiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

	if len(msg.DBName) == 0 {
		return errors.New("empty database name")
	}

	if len(msg.MetricName) == 0 {
		return errors.New("empty metric name")
	}

	if len(msg.Data) == 0 {
		return errors.New("empty data")
	}

	exists, err := r.DBExists(msg.DBName)
	if err != nil {
		return err
	}

	if !exists {
		err = r.AddDatabase(msg.DBName)
		return err
	}

	for _, data := range msg.Data {
		// Json data
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}

		// Get buffer
		buffer := bytes.NewReader(jsonData)
		var partMiBs int64 = 10

		// Setup uploader
		uploader := manager.NewUploader(r.S3Client, func(u *manager.Uploader) {
			u.PartSize = partMiBs * 1024 * 1024
		})

		objectKey := msg.DBName + "_" + strconv.FormatInt(time.Now().UTC().Unix(), 10)

		// Upload data
		_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(msg.DBName),
			Key:    aws.String(objectKey),
			Body:   buffer,
		})
		if err != nil {
			return err
		}
	}

	return nil
}