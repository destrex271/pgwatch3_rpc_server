package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func initContainer(ctx context.Context) (testcontainers.Container, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:latest",
			ExposedPorts: []string{"9092:9092"},
			WaitingFor:   wait.ForListeningPort("9092"),
			WorkingDir:   "/opt/kafka/bin/",
		},
		Started: true,
	})

	if err != nil {
		return nil, err
	}

	return container, nil
}

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

func TestNewKafkaProducer(t *testing.T) {
	// Create new Producer
	container, err := initContainer(context.Background())
	defer func() {
		t.Log("Terminating test container")
		container.Terminate(context.Background())
	}()

	if err != nil {
		t.Log("FAILING!")
		t.Error(err)
		t.Log(container.Host(context.Background()))
	}

	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)

	assert.Nil(t, err, "Error encountered while create new kafka producer")
	assert.NotNil(t, kpr, "Kafka Producer created")
}

func TestUpdateMeasurements_KAFKA_ValidData(t *testing.T) {
	// Create new Producer
	container, err := initContainer(context.Background())
	defer func() {
		t.Log("Terminating test container...")
		container.Terminate(context.Background())
	}()

	if err != nil {
		t.Fatal(err)
	}

	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)

	assert.Nil(t, err, "Error encountered while create new kafka producer")
	assert.NotNil(t, kpr, "Kafka Producer created")

	// Send dummy measurement to kafka topic
	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = kpr.UpdateMeasurements(msg, logMsg)

	// Check if connection was succesfully established
	assert.Nil(t, err, "Error encoutered while adding new measurements")

	// Try to consume data added to topicc
	cmd := []string{"timeout", "10s", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", "test", "--from-beginning"}
	_, reader, err := container.Exec(context.Background(), cmd)
	// _, reader, err := container.Exec(context.Background(), []string{"ls"})

	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		t.Fatal(err)
	}

	// See the logs from the command execution.
	t.Log(buf)
	msg_as_str, err := json.Marshal(msg)
	assert.True(t, strings.Contains(buf.String(), string(msg_as_str)), "Unable to retrieve measurements from topic")
}

func TestUpdateMeasurements_KAFKA_EmptyDatabase(t *testing.T) {
	// Create new Producer
	container, err := initContainer(context.Background())
	defer func() {
		t.Log("Terminating test container")
		container.Terminate(context.Background())
	}()

	if err != nil {
		t.Fatal(err)
	}

	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)

	assert.Nil(t, err, "Error encountered while create new kafka producer")
	assert.NotNil(t, kpr, "Kafka Producer created")

	// Send dummy measurement to kafka topic
	msg := getMeasurementEnvelope()
	msg.DBName = ""
	logMsg := new(string)
	err = kpr.UpdateMeasurements(msg, logMsg)

	// Check if connection was succesfully established
	assert.NotNil(t, err, "No error generated!")
	assert.EqualError(t, err, "Empty Database", "Unexpected error message received")
}

func TestUpdateMeasurements_KAFKA_EmptyMetricName(t *testing.T) {
	// Create new Producer
	container, err := initContainer(context.Background())
	defer func() {
		t.Log("Terminating test container")
		container.Terminate(context.Background())
	}()

	if err != nil {
		t.Fatal(err)
	}

	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)

	assert.Nil(t, err, "Error encountered while create new kafka producer")
	assert.NotNil(t, kpr, "Kafka Producer created")

	// Send dummy measurement to kafka topic
	msg := getMeasurementEnvelope()
	msg.MetricName = ""
	logMsg := new(string)
	err = kpr.UpdateMeasurements(msg, logMsg)

	// Check if connection was succesfully established
	assert.NotNil(t, err, "No error generated!")
	assert.EqualError(t, err, "Empty Metric Name", "Unexpected error message received")
}
