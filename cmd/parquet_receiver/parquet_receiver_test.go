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
	defer func () { _ = os.RemoveAll(fullPath + "/parquet_readings") }()

	msg := GetTestMeasurementEnvelope()
	recv := NewParquetReceiver(fullPath)
	_, err = os.Stat(fullPath + "/parquet_readings")
	assert.False(t, os.IsNotExist(err), "Measurements Directory does not exist")

	_, err = recv.UpdateMeasurements(context.Background(), msg)
	dbFilePath := fullPath + "/parquet_readings/" + msg.GetDBName() + ".parquet"

	assert.NoError(t, err)
	assert.FileExists(t, dbFilePath, "Database Parquet file not found")
}