package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

var (
	ctx context.Context = context.Background()
	llamaConnectionStr string
	pgConnectionStr string
)

func TestMain(m *testing.M) {
	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		panic(err)
	}

	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		panic(err)
	}

	defer func() {
		_ = ollamaContainer.Terminate(ctx)
		_ = postgresContainer.Terminate(ctx)
	}()

	// wait a little to ensure full startup of containers
	time.Sleep(5 * time.Second)

	llamaConnectionStr, err = ollamaContainer.ConnectionString(ctx)
	if err != nil {
		panic(err)
	}

	pgConnectionStr, err = postgresContainer.ConnectionString(ctx)
	if err != nil {
		panic(err)
	}

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Tests start from here

func TestLLamaReceiver(t *testing.T) {
	var err error
	recv, err := NewLLamaReceiver(llamaConnectionStr, pgConnectionStr, ctx, 1)
	assert.NotNil(t, recv, "Receiver object is nil")
	assert.NoError(t, err, "Error encountered while creating receiver")

	conn, err := recv.ConnPool.Acquire(recv.Ctx)
	assert.NoError(t, err, "error encountered while acquiring new connection")
	assert.NotNil(t, conn, "connection obtained is nil")

	msg := testutils.GetTestMeasurementEnvelope()

	t.Run("check setuped tables", func(t *testing.T) {
		dbs := [3]string{"db", "measurements", "insights"}
		var doesExist bool
		for _, db := range dbs {
			query := fmt.Sprintf("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '%s');", db)

			err := conn.QueryRow(recv.Ctx, query).Scan(&doesExist)
			assert.NoError(t, err, "error querying information_schema table")
			assert.Truef(t, doesExist, "table %s does not exist", db)
		}
	})

	t.Run("Update Measurements", func(t *testing.T) {
		_, err := recv.UpdateMeasurements(ctx, msg)
		assert.NoError(t, err, "error encountered while updating measurements")

		// Check insights table for new entry
		newInsightsCount := 0
		recv.InsightsGenerationWg.Wait()
		err = conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
		
		assert.NoError(t, err)
		assert.Equal(t, newInsightsCount, 1, "No new entries inserted in insights table")
	})

	t.Run("Update Measurements Multiple", func(t *testing.T) {
		for range 10 {
			_, err := recv.UpdateMeasurements(ctx, msg)
			assert.NoError(t, err, "error encountered while updating measurements")
		}

		newInsightsCount := 0
		recv.InsightsGenerationWg.Wait()
		err := conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
		
		assert.NoError(t, err)
		assert.Greater(t, newInsightsCount, 1, "No new entries inserted in insights table")
	})

	t.Run("LLama SyncMetricHandler", func(t *testing.T) {
		var exists bool
		req := testutils.GetTestRPCSyncRequest()
		query := fmt.Sprintf("SELECT EXISTS (SELECT * FROM db WHERE dbname = '%s')", req.GetDBName())

		_, err = recv.SyncMetric(ctx, req)
		assert.NoError(t, err)
		time.Sleep(time.Second) // give some time for handler

		err := conn.QueryRow(ctx, query).Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists)

		req.Operation = pb.SyncOp_DeleteOp
		_, err = recv.SyncMetric(ctx, req)
		assert.NoError(t, err)
		time.Sleep(time.Second) // give some time for handler

		err = conn.QueryRow(ctx, query).Scan(&exists)
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}