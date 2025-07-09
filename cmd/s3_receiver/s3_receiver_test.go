package main

import (
	"context"
	"fmt"
	"log"
	"testing"

	
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"google.golang.org/protobuf/types/known/structpb"
)

func GetTestMeasurementEnvelope() *pb.MeasurementEnvelope {
	st, err := structpb.NewStruct(map[string]any{"key": "val"})
	if err != nil {
		panic(err)
	}
	measurements := []*structpb.Struct{st}
	return &pb.MeasurementEnvelope{
		DBName:           "test",
		MetricName:       "testMetric",
		Data:             measurements,
	}
}

func initContainer(ctx context.Context) (*localstack.LocalStackContainer, error) {
	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:1.4.0")
	if err != nil {
		return nil, err
	}

	return localstackContainer, err
}

func TestS3Receiver(t *testing.T) {
	ctx := context.Background()

	container, err := initContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Println("unable to terminate localstack container : " + err.Error())
		}
	}()

	mappedPort, err := container.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		panic(err)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		panic(err)
	}
	defer func() {_ = provider.Close()}()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		panic(err)
	}

	client, err := NewS3Receiver(fmt.Sprintf("http://%s:%d", host, mappedPort.Int()), "us-east-1", "test", "test")
	assert.NoError(t, err, "error encountered while creating S3Receiver")
	assert.NotNil(t, client, "received nil instead of client")

	t.Run("Add Database", func(t *testing.T) {
		dbname := "test-db"
		err = client.AddDatabase(dbname)
		assert.NoError(t, err)

		res, err := client.DBExists(dbname)
		assert.NoError(t, err, "error encountered while checking bucket")
		assert.True(t, res, "bucket should exist")
	})
	
	t.Run("Update Measurements", func(t *testing.T) {
		msg := GetTestMeasurementEnvelope()
		_, err = client.UpdateMeasurements(ctx, msg)
		assert.NoError(t, err, "error encountered while updating measurements")

		newCtx, cancel := context.WithCancel(ctx)
		cancel()
		reply, err := client.UpdateMeasurements(newCtx, msg)
		assert.Equal(t, reply.GetLogmsg(), "context cancelled, stopping writer...")
		assert.NoError(t, err)
	})
}