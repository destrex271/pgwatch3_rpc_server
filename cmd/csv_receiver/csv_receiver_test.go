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
	recv := NewCSVReceiver(fullPath)

	// Call Update Measurements with dummy data
	msg := testutils.GetTestMeasurementEnvelope()
	_, err = recv.UpdateMeasurements(context.Background(), msg)
	assert.NoError(t, err)
	defer func() { _ = os.RemoveAll(fullPath + "/" + msg.DBName) }()

	// Check if database folder and metric files are created 
	dbDir := fullPath + "/" + msg.GetDBName()
	_, err = os.Stat(dbDir)
	assert.False(t, os.IsNotExist(err), "Database Directory does not exist")

	metricFile := dbDir + msg.GetMetricName() + ".csv"
	assert.FileExistsf(t, metricFile, "CSV file for metric %s doesn't exist", msg.GetMetricName())
}