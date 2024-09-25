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

func TestUpdateMeasurements(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		panic("Error: " + err.Error())
	}

	// Get TextReceiver
	recv := &TextReceiver{
		FullPath:          path,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	logMsg := new(string)
	msg := getMeasurementEnvelope()
	err = recv.UpdateMeasurements(msg, logMsg)

	// Check if there are any errors
	assert.Nil(t, err, "Error encountered while updating measurements")

	// Check if file was created
	assert.FileExists(t, path+"/"+msg.DBName+".txt", "Database file does not exist")

	err = os.Remove(path + "/" + msg.DBName + ".txt")
	if err != nil {
		t.Error(err)
	}
}
