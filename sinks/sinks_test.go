package sinks

import (
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
)

func getTestRPCSyncRequest() *api.RPCSyncRequest {
	return &api.RPCSyncRequest{
		DbName:     "test_database",
		MetricName: "test_metric",
		Operation:  "ADD",
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

func TestSyncMetric_EmptyOperation(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.Operation = ""

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "Empty Operation.")
}

func TestSyncMetric_EmptyDatabase(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
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
