package main

import (
	"context"
	"log"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"
)

func initContainer(ctx context.Context) (*tcollama.OllamaContainer, error) {
	ollamaContainer, err := tcollama.Run(ctx, "ollama/ollama:0.1.25")
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return nil, err
	}

	_, _, err = ollamaContainer.Exec(ctx, []string{"ollama", "pull", "tinyllama"})
	if err != nil {
		log.Println("unable to pull llama3: " + err.Error())
		return nil, err
	}

	_, _, err = ollamaContainer.Exec(ctx, []string{"ollama", "run", "tinyllama"})
	if err != nil {
		log.Println("unable to run llama3: " + err.Error())
	}

	return ollamaContainer, nil
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

func TestNewLlamaReceiver(t *testing.T) {
	ctx := context.Background()
	container, err := initContainer(ctx)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Create new receiver
	connectionStr, err := container.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		t.Fatal(err)
	}

	recv, err := NewLlamaReceiver(connectionStr)

	assert.NotNil(t, recv, "Receiver object is nil")
	assert.Nil(t, err, "Error encountered while creating receiver")
}

func TestUpdateMeasurements(t *testing.T) {
	recv, _ := NewLlamaReceiver("http://localhost:11434")
	logMsg := new(string)
	recv.UpdateMeasurements(getMeasurementEnvelope(), logMsg)
}
