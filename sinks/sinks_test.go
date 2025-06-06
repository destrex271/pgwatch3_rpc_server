package sinks

import (
	"testing"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
)

func getTestRPCSyncRequest() *api.RPCSyncRequest {
	return &api.RPCSyncRequest{
		DbName:     "test_database",
		MetricName: "test_metric",
		Operation:  api.AddOp,
	}
}

func TestNewSyncMetricHandler(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")
}

func TestSyncMetric_ValidData(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.Nil(t, err, "Encoutnered an Error")
}

func TestSyncMetric_InvalidOperation(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.Operation = -1 

	// Send data to Sync Metric Handler and check if it returns any error
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "Invalid Operation type.")
}

func TestSyncMetric_EmptyDatabase(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(0)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.DbName = ""

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "Empty Database.")
}

func TestSyncMetric_EmptyMetric(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.MetricName = ""

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "Empty Metric Provided.")
}

func TestHandleSyncMetric(t *testing.T) {
	handler := NewSyncMetricHandler(1024)
	// handler routine
	go handler.HandleSyncMetric()

	logMsg := "test msg"
	for range 10 {
		// issue a channel write
		handler.SyncMetric(getTestRPCSyncRequest(), &logMsg)
		time.Sleep(10 * time.Millisecond)
		// Ensure the Channel has been emptied
		assert.Equal(t, len(handler.SyncChannel), 0)
	}
}