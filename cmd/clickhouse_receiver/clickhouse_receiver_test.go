package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/structpb"
)

func initContainer(ctx context.Context, User string, Password string, DBName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.7",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Logging errors to").WithStartupTimeout(2 * time.Minute),
		Env: map[string]string{
			"CLICKHOUSE_DB":       DBName,
			"CLICKHOUSE_USER":     User,
			"CLICKHOUSE_PASSWORD": Password,
		},
	}

	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	// Delay to allow container to be ready
	time.Sleep(5 * time.Second)
	return clickhouseContainer, err
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

var ctx context.Context = context.Background()
var User string = "clickhouse"
var Password string = "password"
var DBName string = "testdb"
var serverURI string

func TestMain(m *testing.M) {
	container, err := initContainer(ctx, User, Password, DBName)
	if err != nil {
		panic(err)
	}
	defer func() { _ = container.Terminate(ctx) }()

	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		panic(err)
	}
	serverURI = fmt.Sprintf("127.0.0.1:%d", mappedPort.Int())

	m.Run()
}

// Tests begin from here

func TestGetConnection(t *testing.T) {
	conn, err := GetConnection(User, Password, DBName, serverURI, true)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestClickHouseReceiver(t *testing.T) {
	recv, err := NewClickHouseReceiver(User, Password, DBName, serverURI, true)
	assert.NoError(t, err)
	assert.NotNil(t, recv, "Receiver not created")

	_, err = recv.Conn.Query(ctx, "select * from Measurements;")
	assert.NoError(t, err, "Measurements table not generated")

	data := GetTestMeasurementEnvelope()
	_, err = recv.UpdateMeasurements(ctx, data)
	assert.NoError(t, err)

	rows, _ := recv.Conn.Query(ctx, "select * from Measurements;")
	rowCount := 0
	for rows.Next() {
		var dbname, metric_name, data string
		tags := make(map[string]string)
		var tt time.Time

		err := rows.Scan(&dbname, &metric_name, &tags, &data, &tt)
		assert.NoError(t, err, "Failed to scan row")
		rowCount++
	}
	assert.Greater(t, rowCount, 0, "No Rows inserted")
}