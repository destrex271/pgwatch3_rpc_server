package main

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func initContainer(ctx context.Context, user string, password string, dbname string) (testcontainers.Container, error) {

	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:23.3.8.21-alpine",
		clickhouse.WithUsername(user),
		clickhouse.WithPassword(password),
		clickhouse.WithDatabase(dbname),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				ExposedPorts: []string{"9000:9000/tcp", "8123:8123/tcp", "8443:8443/tcp", "9440:9440/tcp"},
			},
		}),
	)

	if err != nil {

		return nil, err
	}

	return clickHouseContainer, nil
}

func TestNewClickHouse(t *testing.T) {

	// Variables
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

	// Create new Receiver
	mappedPort, err := container.MappedPort(context.Background(), "8123/tcp")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	// Get the host
	host, err := container.Host(context.Background())
	if err != nil {
		t.Fatalf("Failed to get host: %v", err)
	}

	// Create new Receiver
	recv, err := NewClickHouseReceiver(user, password, fmt.Sprintf("%s:%s", host, mappedPort.Port()))
	// recv, err := NewClickHouseReceiver(user, password, "localhost:8123")
	log.Println(err)

	assert.Nil(t, err, "Encountered error while creating new receiver")
	assert.NotNil(t, recv, "Receiver not created")

	data, err := recv.Conn.Query("select * from measurements;")

	if err != nil {
		t.Fatal(err)
	}

	var dc []interface{}
	for data.Next() {
		data.Scan(&dc)
		fmt.Println(dc)
	}

	assert.Len(t, data, 100000, "Data is ")
}
