package main

import (
	"context"
	"os"
	"testing"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func getMeasurementEnvelope() *pb.MeasurementEnvelope {
	var measurements []*structpb.Struct
	return &pb.MeasurementEnvelope{
		DBName:           "test",
		MetricName:       "testMetric",
		CustomTags:       nil,
		Data:             measurements,
	}
}

func TestUpdateMeasurements(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		panic("Error: " + err.Error())
	}

	// Get TextReceiver
	recv := NewTextReceiver(path)

	msg := getMeasurementEnvelope()
	_, err = recv.UpdateMeasurements(context.Background(), msg)

	// Check if there are any errors
	assert.Nil(t, err, "Error encountered while updating measurements")

	// Check if file was created
	assert.FileExists(t, path+"/"+msg.DBName+".txt", "Database file does not exist")

	err = os.Remove(path + "/" + msg.DBName + ".txt")
	if err != nil {
		t.Error(err)
	}
}
