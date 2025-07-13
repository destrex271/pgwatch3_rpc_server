package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/structpb"
)

func initContainer(ctx context.Context) (testcontainers.Container, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:latest",
			ExposedPorts: []string{"9092:9092"},
			WaitingFor:   wait.ForLog("Kafka Server started").WithStartupTimeout(120 * time.Second),
			WorkingDir:   "/opt/kafka/bin/",
		},
		Started: true,
	})

	if err != nil {
		return nil, err
	}

	return container, nil
}

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

func GetTestRPCSyncRequest() *pb.SyncReq {
	return &pb.SyncReq{
		DBName:     "test_db",
		MetricName: "test_metric",
		Operation:  pb.SyncOp_AddOp,
	}
}

var container testcontainers.Container
var err error
var ctx = context.Background()

func TestMain(m *testing.M) {
	container, err = initContainer(ctx)
	if err != nil {
		panic(err)
	}

	defer func() {
		_ = container.Terminate(ctx)
	}()

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Tests begin from here

func TestKafka_UpdateMeasurements(t *testing.T) {
	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)
	require.NoError(t, err, "Error encountered while creating kafka producer")
	require.NotNil(t, kpr, "Kafka Producer object is nil")

	msg := GetTestMeasurementEnvelope()
	_, err = kpr.UpdateMeasurements(ctx, msg)
	assert.NoError(t, err, "Error encountered while updating measurements")

	// Try to consume data added to topic
	cmd := []string{"timeout", "10s", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", "test", "--from-beginning"}
	_, reader, err := container.Exec(ctx, cmd)
	assert.NoError(t, err)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	assert.NoError(t, err)

	msg_as_str := sinks.GetJson(msg)
	assert.True(t, strings.Contains(buf.String(), msg_as_str), "Unable to retrieve measurements from topic")
}

func TestKafka_SyncMetricHandler(t *testing.T) {
	kpr, err := NewKafkaProducer("localhost:9092", nil, nil, true)
	require.NoError(t, err, "Error encountered while creating kafka producer")
	require.NotNil(t, kpr, "Kafka Producer object is nil")

	req := GetTestRPCSyncRequest()
	
	kpr.SyncMetric(ctx, req)
	// give some time for handler routine
	time.Sleep(time.Second)
	_, exists := kpr.conn_regisrty[req.GetDBName()]
	assert.True(t, exists)

	req.Operation = pb.SyncOp_DeleteOp

	kpr.SyncMetric(ctx, req)
	// give some time for handler routine
	time.Sleep(time.Second)
	_, exists = kpr.conn_regisrty[req.GetDBName()]
	assert.False(t, exists)
}