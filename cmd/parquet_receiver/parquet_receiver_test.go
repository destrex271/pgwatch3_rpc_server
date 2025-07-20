package main

import (
	"context"
	"os"
	"testing"

	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
)

func TestUpdateMeasurements(t *testing.T) {
	fullPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	defer func () { _ = os.RemoveAll(fullPath + "/parquet_readings") }()

	msg := testutils.GetTestMeasurementEnvelope()
	recv := NewParquetReceiver(fullPath)
	_, err = os.Stat(fullPath + "/parquet_readings")
	assert.False(t, os.IsNotExist(err), "Measurements Directory does not exist")

	_, err = recv.UpdateMeasurements(context.Background(), msg)
	dbFilePath := fullPath + "/parquet_readings/" + msg.GetDBName() + ".parquet"

	assert.NoError(t, err)
	assert.FileExists(t, dbFilePath, "Database Parquet file not found")
}