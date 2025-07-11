package main

import (
	"context"
	"os"
	"testing"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

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

func TestUpdateMeasurements(t *testing.T) {
	fullPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	recv := NewCSVReceiver(fullPath)

	// Call Update Measurements with dummy data
	msg := GetTestMeasurementEnvelope()
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