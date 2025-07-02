package main

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func getMeasurementEnvelope() *api.MeasurementEnvelope {
	measurement := make(map[string]any)
	measurement["cpu"] = "0.001"
	measurement["checkpointer"] = "1"
	var measurements []map[string]any
	measurements = append(measurements, measurement)

	sql := make(map[int]string)
	sql[12] = "select * from abc;"
	metrics := &api.Metric{
		SQLs:        sql,
		InitSQL:     "select * from abc;",
		NodeStatus:  "healthy",
		StorageName: "teststore",
		Description: "test metric",
	}

	return &api.MeasurementEnvelope{
		DBName:           "test",
		SourceType:       "test_source",
		MetricName:       "testMetric",
		CustomTags:       nil,
		Data:             measurements,
		MetricDef:        *metrics,
		RealDbname:       "test",
		SystemIdentifier: "Identifier",
	}
}

func initContainer(ctx context.Context) (*localstack.LocalStackContainer, error) {
	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:1.4.0")
	if err != nil {
		return nil, err
	}

	return localstackContainer, err
}

func TestNewS3Receiver(t *testing.T) {

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
		t.Fatal(err)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {_ = provider.Close()}()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewS3Receiver(fmt.Sprintf("http://%s:%d", host, mappedPort.Int()), "us-east-1", "test", "test")

	assert.Nil(t, err, "error encoutnered while creating S3Receiver")
	assert.NotNil(t, client, "received nil instead of client")
}

func TestAddDatabase(t *testing.T) {

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
		t.Fatal(err)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {_ = provider.Close()}()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewS3Receiver(fmt.Sprintf("http://%s:%d", host, mappedPort.Int()), "us-east-1", "test", "test")

	assert.Nil(t, err, "error encoutnered while creating S3Receiver")
	assert.NotNil(t, client, "received nil instead of client")

	dbname := "test-db"
	err = client.AddDatabase(dbname)
	assert.NoError(t, err)

	res, err := client.DBExists(dbname)

	assert.Nil(t, err, "error encountered while checking bucket")
	assert.True(t, res, "bucket should exist")
}

func TestUpdateMeasurements(t *testing.T) {
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
		t.Fatal(err)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {_ = provider.Close()}()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewS3Receiver(fmt.Sprintf("http://%s:%d", host, mappedPort.Int()), "us-east-1", "test", "test")

	assert.Nil(t, err, "error encoutnered while creating S3Receiver")
	assert.NotNil(t, client, "received nil instead of client")

	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = client.UpdateMeasurements(msg, logMsg)

	assert.Nil(t, err, "error encountered while updating measurements")
}