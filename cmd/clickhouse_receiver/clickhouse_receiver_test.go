package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
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
	time.Sleep(30 * time.Second)
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
		CustomTags: 	  map[string]string{"tagName": "tagValue"},
		Data:             measurements,
	}
}

var (
	ctx context.Context = context.Background()
	User string = "clickhouse"
	Password string = "password"
	DBName string = "testdb"
	serverURI string
)

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

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Tests begin from here

func TestGetConnection(t *testing.T) {
	conn, err := GetConnection(User, Password, DBName, serverURI, true)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestClickHouseReceiver(t *testing.T) {
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

	recv, err := NewClickHouseReceiver(User, Password, DBName, serverURI, true)
	assert.NoError(t, err)
	assert.NotNil(t, recv, "error creating clickhouse receiver")

	msg := GetTestMeasurementEnvelope()

	for cnt := range 5 {
		_, err = recv.UpdateMeasurements(ctx, msg)
		assert.NoError(t, err)

		rows, err := recv.Conn.Query(ctx, "select * from Measurements;")
		assert.NoError(t, err)

		rowCount := 0
		for rows.Next() {
			var dbname, metric_name, data string
			tags := make(map[string]string)
			var timestamp time.Time
			dataJson := sinks.GetJson(msg.GetData()[0])

			err := rows.Scan(&dbname, &metric_name, &tags, &data, &timestamp)
			assert.NoError(t, err, "Failed to scan row")
			assert.Equal(t, dbname, msg.GetDBName())
			assert.Equal(t, metric_name, msg.GetMetricName())
			assert.True(t, reflect.DeepEqual(tags, msg.GetCustomTags()))
			assert.Equal(t, data, dataJson)

			rowCount++
		}
		assert.Equalf(t, rowCount, cnt + 1, "Expected %v rows found %v", cnt + 1, rowCount)
	}
}