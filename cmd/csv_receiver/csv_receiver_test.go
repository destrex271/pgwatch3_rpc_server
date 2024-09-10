package main

import (
	"os"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
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

func TestUpdateMeasurements_CSV_ValidData(t *testing.T) {

	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	sync_handler := sinks.NewSyncMetricHandler(1024)

	// Create new CSV Receiver
	recv := &CSVReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sync_handler,
	}

	// Call Update Measurements with dummy packet
	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	assert.Nil(t, err, *logMsg)

	// Check folder structure and metric file

	// Check if root folder created for database
	if _, err := os.Stat(fullPath + "/" + msg.DBName); err != nil {
		assert.False(t, os.IsNotExist(err), "Directory does not exist")
	}

	// Check if csv file for metric exists
	assert.FileExists(t, fullPath+"/"+msg.DBName+"/"+msg.MetricName+".csv", "CSV file not found for metric")

	// Cleanup
	os.RemoveAll(fullPath + "/" + msg.DBName)
}

func TestUpdateMeasurements_CSV_EmptyDBName(t *testing.T) {

	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	sync_handler := sinks.NewSyncMetricHandler(1024)

	// Create new CSV Receiver
	recv := &CSVReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sync_handler,
	}

	// Call Update Measurements with dummy packet
	msg := getMeasurementEnvelope()
	msg.DBName = ""
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	assert.NotNil(t, err, "No error thrown for empty Database Name")
	assert.EqualError(t, err, "Empty Database", "Invalid Error Message")
}

func TestUpdateMeasurements_CSV_EmptyMetricName(t *testing.T) {
	fullPath, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	sync_handler := sinks.NewSyncMetricHandler(1024)

	// Create new CSV Receiver
	recv := &CSVReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sync_handler,
	}

	// Call Update Measurements with dummy packet
	msg := getMeasurementEnvelope()
	msg.MetricName = ""
	logMsg := new(string)
	err = recv.UpdateMeasurements(msg, logMsg)

	assert.NotNil(t, err, "No error thrown for empty Metric Name")
	assert.EqualError(t, err, "Unidentifiable Metric Name: EMPTY", "Invalid Error Message for empty metric name")
}
