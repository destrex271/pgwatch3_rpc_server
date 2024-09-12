package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func initContainer(ctx context.Context, user string, password string, dbname string) (testcontainers.Container, error) {

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForListeningPort("9000/tcp"),
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
	conn, err := GetConnection(user, password, dbname, serverUri)

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
	recv, err := NewClickHouseReceiver(user, password, dbname, uri)

	assert.Nil(t, err, "Encountered error while creating new receiver")
	assert.NotNil(t, recv, "Receiver not created")

	// Check if table was created/exists
	_, err = recv.Conn.Query(ctx, "select * from Measurements;")

	assert.Nil(t, err, "Measurements table not generated")
}
