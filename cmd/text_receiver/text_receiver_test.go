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
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	recv := NewTextReceiver(path)
	msg := GetTestMeasurementEnvelope()
	_, err = recv.UpdateMeasurements(context.Background(), msg)

	// Check if there are any errors
	assert.NoError(t, err, "Error encountered while updating measurements")

	// Check if file was created
	assert.FileExists(t, path + "/" + msg.DBName + ".txt", "Database file does not exist")
	_ = os.Remove(path + "/" + msg.DBName + ".txt")
}