package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/structpb"
)

const new_image = "tinyllama_image"

func initOllamaContainer(ctx context.Context) (*tcollama.OllamaContainer, error) {
	ollamaContainer, err := tcollama.Run(ctx, new_image)
	if err != nil {
		ollamaContainer, err = tcollama.Run(ctx, "ollama/ollama:0.1.25")
		if err != nil {
			log.Printf("failed to start container: %s", err)
			return nil, err
		}

		// Pull model and commit container
		_, _, err = ollamaContainer.Exec(ctx, []string{"ollama", "pull", "tinyllama"})
		if err != nil {
			log.Println("unable to pull llama3: " + err.Error())
			return nil, err
		}
		_ = ollamaContainer.Commit(ctx, new_image)
	}

	return ollamaContainer, nil
}

func initPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, error) {
	dbName := "postgres"
	dbUser := "postgres"
	dbPassword := "postgres"

	postgresContainer, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)

	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
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

var connectionStr string
var pgConnectionStr string
var ctx context.Context

func TestMain(m *testing.M) {
	ctx := context.Background()
	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		panic(err)
	}
	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	connectionStr, err = ollamaContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		panic(err)
	}

	pgConnectionStr, err = postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get Postgres connection string")
		panic(err)
	}

	m.Run()
}

// Tests begin from here

func TestLLamaReceiver(t *testing.T) {
	recv, err := NewLLamaReceiver(connectionStr, pgConnectionStr, ctx, 10)
	assert.NotNil(t, recv, "Receiver object is nil")
	assert.NoError(t, err, "Error encountered while creating receiver")

	conn, err := recv.ConnPool.Acquire(recv.Ctx)
	assert.NoError(t, err, "error encountered while acquiring new connection")
	assert.NotNil(t, conn, "connection obtained in nil")
	defer conn.Release()

	t.Run("Check Tables", func(t *testing.T) {
		tables := [...]string{"db", "measurements", "insights"}
		var doesExist bool
		for _, table := range tables {
			err = conn.QueryRow(recv.Ctx, 
				fmt.Sprintf(`SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE  table_name   = %s
			);`, table)).Scan(&doesExist)

			assert.NoError(t, err, "error encountered while querying table")
			assert.True(t, doesExist, fmt.Sprintf("table %s does not exist", table))
		}
	})

	t.Run("UpdateMeasurements", func(t *testing.T) {
		msg := GetTestMeasurementEnvelope()
		_, err = recv.UpdateMeasurements(ctx, msg)
		assert.NoError(t, err, "error encountered while updating measurements")

		// Check insights table for new entry
		newInsightsCount := 0
		recv.InsightsGenerationWg.Wait()
		err = conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
		assert.NoError(t, err)
		assert.Equal(t, newInsightsCount, 1, "No new entries inserted in insights table")
	})

	t.Run("UpdateMeasurements Multiple", func(t *testing.T) {
		msg := GetTestMeasurementEnvelope()
		for range 10 {
			_, err = recv.UpdateMeasurements(context.Background(), msg)
			assert.NoError(t, err, "error encountered while updating measurements")
		}

		newInsightsCount := 0
		recv.InsightsGenerationWg.Wait()
		err = conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
		assert.NoError(t, err)
		assert.Equal(t, newInsightsCount, 10, "No new entries inserted in insights table")
	})
}