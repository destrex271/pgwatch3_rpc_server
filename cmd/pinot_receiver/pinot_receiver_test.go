package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/stretchr/testify/assert"
)

// mockHTTPServer creates a mock Pinot controller server for testing
func mockHTTPServer() *httptest.Server {
	handler := http.NewServeMux()

	// Mock schema upload endpoint
	handler.HandleFunc("/schemas", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Mock table creation endpoint
	handler.HandleFunc("/tables", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Mock ingest endpoint
	handler.HandleFunc("/ingestFromFile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	return httptest.NewServer(handler)
}

func setupTestConfigDir(t *testing.T) (string, func()) {
	tempDir, err := os.MkdirTemp("", "pinot-test-")
	assert.NoError(t, err, "Failed to create temp directory")

	// Create valid schema.json
	schemaJSON := `{
		"schemaName": "pgwatch_metrics",
		"dimensionFieldSpecs": [
			{
				"name": "dbname",
				"dataType": "STRING",
				"defaultNullValue": ""
			},
			{
				"name": "metric_name",
				"dataType": "STRING",
				"defaultNullValue": ""
			}
		],
		"dateTimeFieldSpecs": [
			{
				"name": "timestamp",
				"dataType": "TIMESTAMP",
				"format": "1:MILLISECONDS:EPOCH",
				"granularity": "1:MILLISECONDS"
			}
		]
	}`

	// Create valid table.json
	tableJSON := `{
		"tableName": "pgwatch_metrics",
		"tableType": "OFFLINE",
		"segmentsConfig": {
			"timeColumnName": "timestamp",
			"timeType": "MILLISECONDS"
		}
	}`

	err = os.WriteFile(filepath.Join(tempDir, "schema.json"), []byte(schemaJSON), 0644)
	assert.NoError(t, err, "Failed to write schema.json")

	err = os.WriteFile(filepath.Join(tempDir, "table.json"), []byte(tableJSON), 0644)
	assert.NoError(t, err, "Failed to write table.json")

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

// getMeasurementEnvelope creates a test measurement envelope
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

func TestNewPinotReceiver(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Test valid initialization
	receiver, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")
	assert.NotNil(t, receiver, "Receiver should not be nil")

	// Test with invalid config dir
	_, err = NewPinotReceiver(server.URL, "pgwatch_metrics", "/non/existent/dir")
	assert.Error(t, err, "Should error with non-existent config dir")
	assert.Contains(t, err.Error(), "config directory", "Error should mention config directory")
}

func TestInitializePinotTable(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	_, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")

	// Test is already covered by initialization, but we can add additional test cases:

	// Test with missing schema file
	_ = os.Remove(filepath.Join(configDir, "schema.json"))
	receiver := &PinotReceiver{
		ControllerURL:     server.URL,
		TableName:         "pgwatch_metrics",
		ConfigDir:         configDir,
		Client:            &http.Client{},
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}
	err = receiver.initializePinotTable()
	assert.Error(t, err, "Should error with missing schema file")
	assert.Contains(t, err.Error(), "schema config file not found", "Error should mention missing schema file")
}

func TestUpdateMeasurements_ValidData(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	receiver, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")

	// Test valid measurement update
	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = receiver.UpdateMeasurements(msg, logMsg)
	assert.NoError(t, err, "Valid measurement update should succeed")
	assert.Contains(t, *logMsg, "Successfully inserted batch", "Log message should indicate success")
}

func TestUpdateMeasurements_EmptyDBName(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	receiver, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")

	// Test empty DB name
	msg := getMeasurementEnvelope()
	msg.DBName = ""
	logMsg := new(string)
	err = receiver.UpdateMeasurements(msg, logMsg)
	assert.Error(t, err, "Empty DB name should cause error")
	assert.Equal(t, "empty database name", *logMsg, "Error message should mention empty database name")
}

func TestUpdateMeasurements_EmptyMetricName(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	receiver, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")

	// Test empty metric name
	msg := getMeasurementEnvelope()
	msg.MetricName = ""
	logMsg := new(string)
	err = receiver.UpdateMeasurements(msg, logMsg)
	assert.Error(t, err, "Empty metric name should cause error")
	assert.Equal(t, "empty metric name", *logMsg, "Error message should mention empty metric name")
}

func TestUpdateMeasurements_EmptyData(t *testing.T) {
	server := mockHTTPServer()
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	receiver, err := NewPinotReceiver(server.URL, "pgwatch_metrics", configDir)
	assert.NoError(t, err, "NewPinotReceiver should initialize without error")

	// Test empty data
	msg := getMeasurementEnvelope()
	msg.Data = []map[string]any{}
	logMsg := new(string)
	err = receiver.UpdateMeasurements(msg, logMsg)
	assert.Error(t, err, "Empty data should cause error")
	assert.Equal(t, "no measurements", *logMsg, "Error message should mention no measurements")
}

func TestPinotAPIErrors(t *testing.T) {
	// Create a server that always returns errors
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Pinot API Error", http.StatusInternalServerError)
	}))
	defer server.Close()

	configDir, cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Test schema upload error
	receiver := &PinotReceiver{
		ControllerURL:     server.URL,
		TableName:         "pgwatch_metrics",
		ConfigDir:         configDir,
		Client:            &http.Client{},
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	err := receiver.uploadSchema(filepath.Join(configDir, "schema.json"))
	assert.Error(t, err, "Should error when Pinot API returns error")
	assert.Contains(t, err.Error(), "failed to upload schema", "Error should mention schema upload failure")

	// Test table creation error
	err = receiver.createTable(filepath.Join(configDir, "table.json"))
	assert.Error(t, err, "Should error when Pinot API returns error")
	assert.Contains(t, err.Error(), "failed to create table", "Error should mention table creation failure")
}
