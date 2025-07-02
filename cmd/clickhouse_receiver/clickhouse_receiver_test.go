package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func initContainer(ctx context.Context, user string, password string, dbname string) (testcontainers.Container, error) {

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.7",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Logging errors to").WithStartupTimeout(2 * time.Minute),
		Env: map[string]string{
			"CLICKHOUSE_DB":       dbname,
			"CLICKHOUSE_USER":     user,
			"CLICKHOUSE_PASSWORD": password,
		},
	}

	// Create and start the container
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	// Delay to allow container to be ready
	time.Sleep(5 * time.Second)

	return clickhouseContainer, nil
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

func TestGetConnection(t *testing.T) {

	// Variables
	ctx := context.Background()
	user := "clickhouse"
	password := "password"
	dbname := "testdb"

	// Create new container
	container, err := initContainer(ctx, user, password, dbname)
	if err != nil {
		t.Fatal("[ERROR]: unable to create container. " + err.Error())
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			panic(err)
		}
	}()

	mappedPort, err := container.MappedPort(context.Background(), "9000")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %s", err)
	}
	serverUri := fmt.Sprintf("127.0.0.1:%d", mappedPort.Int())
	conn, err := GetConnection(user, password, dbname, serverUri, true)

	assert.Nil(t, err, "ecountered error while getting connection")
	assert.NotNil(t, conn, "conn is null")
}

func TestNewClickHouse(t *testing.T) {

	// Variables
	ctx := context.Background()
	user := "clickhouse"
	password := "password"
	dbname := "testdb"

	container, err := initContainer(context.Background(), user, password, dbname)
	if err != nil {
		t.Fatal("[ERROR]: unable to create container. " + err.Error())
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			panic(err)
		}
	}()

	mappedPort, err := container.MappedPort(context.Background(), "9000")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %s", err)
	}

	uri := fmt.Sprintf("127.0.0.1:%d", mappedPort.Int())
	recv, err := NewClickHouseReceiver(user, password, dbname, uri, true)

	assert.Nil(t, err, "Encountered error while creating new receiver")
	assert.NotNil(t, recv, "Receiver not created")

	// Check if table was created/exists
	_, err = recv.Conn.Query(ctx, "select * from Measurements;")

	assert.Nil(t, err, "Measurements table not generated")
}

func TestInsertMeasurements(t *testing.T) {

	// Variables
	ctx := context.Background()
	user := "clickhouse"
	password := "password"
	dbname := "testdb"

	// Create Test container
	container, err := initContainer(ctx, user, password, dbname)
	if err != nil {
		t.Fatal("[ERROR]: unable to create container. " + err.Error())
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			panic(err)
		}
	}()

	// test data
	data := getMeasurementEnvelope()

	mappedPort, err := container.MappedPort(context.Background(), "9000")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %s", err)
	}

	uri := fmt.Sprintf("127.0.0.1:%d", mappedPort.Int())
	recv, err := NewClickHouseReceiver(user, password, dbname, uri, true)

	assert.Nil(t, err, "Encountered error while creating new receiver")
	assert.NotNil(t, recv, "Receiver not created")

	// Insert Measurements
	err = recv.InsertMeasurements(data, ctx)
	assert.Nil(t, err, "error encountered while inserting measurements")
}

func TestUpdateMeasurements(t *testing.T) {
	// Variables
	ctx := context.Background()
	user := "clickhouse"
	password := "password"
	dbname := "testdb"

	// Create Test container
	container, err := initContainer(ctx, user, password, dbname)
	if err != nil {
		t.Fatal("[ERROR]: unable to create container. " + err.Error())
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			panic(err)
		}
	}()

	// test data
	data := getMeasurementEnvelope()

	mappedPort, err := container.MappedPort(context.Background(), "9000")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %s", err)
	}

	uri := fmt.Sprintf("127.0.0.1:%d", mappedPort.Int())
	recv, err := NewClickHouseReceiver(user, password, dbname, uri, true)

	assert.Nil(t, err, "Encountered error while creating new receiver")
	assert.NotNil(t, recv, "Receiver not created")

	// Insert Measurements
	msg := new(string)
	err = recv.UpdateMeasurements(data, msg)
	assert.Nil(t, err, "error encountered while updating measurements")
}