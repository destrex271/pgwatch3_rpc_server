package main

import (
	"context"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Receiver struct {
	client *s3.Client
	sinks.SyncMetricHandler
}

func NewS3Receiver(awsEndpoint string, awsRegion string) (*S3Receiver, error) {
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
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
		client:            client,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return recv, nil
}

func (s3 *S3Receiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

	return nil
}
