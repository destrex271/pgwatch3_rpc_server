package main

import (
	"os"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
)

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

func TestUpdateMeasurements_PARQ(t *testing.T) {
	// Get path
	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err.Error())
	}

	// Create Parquet Receiver
	recv := &ParquetReceiver{
		FullPath: fullPath,
	}
	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	// Check if there were any errors while updating measurements
	assert.Nil(t, err, "Received unexpected error")

	// Check folder structure
	if _, err := os.Stat(fullPath + "/parquet_readings"); err != nil {
		assert.False(t, os.IsNotExist(err), "Directory does not exist")
	}

	// Check if csv file for metric exists
	assert.FileExists(t, fullPath+"/parquet_readings/"+msg.DBName+".parquet", "CSV file not found for metric")

	// Cleanup
	_ = os.RemoveAll(fullPath + "/parquet_readings")
}

func TestUpdateMeasurements_PARQ_EmptyDBName(t *testing.T) {
	// Get path
	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err.Error())
	}

	// Create Parquet Receiver
	recv := &ParquetReceiver{
		FullPath: fullPath,
	}
	msg := getMeasurementEnvelope()
	msg.DBName = ""
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	// Check if there were any errors while updating measurements
	assert.EqualError(t, err, "empty database", "Error Message not thrown for empty database name")

	// Check if directories were not created
	if _, err := os.Stat(fullPath + "/parquet_readings"); err != nil {
		assert.True(t, os.IsNotExist(err), "Directory does not exist")
	}
}

func TestUpdateMeasurements_PARQ_EmptyMetricName(t *testing.T) {
	// Get path
	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err.Error())
	}

	// Create Parquet Receiver
	recv := &ParquetReceiver{
		FullPath: fullPath,
	}
	msg := getMeasurementEnvelope()
	msg.MetricName = ""
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	// Check if there were any errors while updating measurements
	assert.EqualError(t, err, "empty metric name", "Error Message not thrown for empty metric name")

	// Check if directories were not created
	if _, err := os.Stat(fullPath + "/parquet_readings"); err != nil {
		assert.True(t, os.IsNotExist(err), "Directory does not exist")
	}
}
