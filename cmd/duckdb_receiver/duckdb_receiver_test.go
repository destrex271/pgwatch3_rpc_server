package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

var testDBPath string
var dbReceiver *DuckDBReceiver

func TestMain(m *testing.M) {
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Print("error in GetCwd: ", err)
		os.Exit(1)
	}
	testDir := filepath.Join(currentDir, "test_tmp")
	err = os.MkdirAll(testDir, 0755)
	if err != nil {
		fmt.Print("error in create test directory: ", err)
		os.Exit(1)
	}

	testDBPath = filepath.Join(testDir, "pgwatch_test.duckdb") // database path

	// run the tests
	code := m.Run()

	// cleanup
	os.Remove(testDBPath)
	os.Remove(testDir)
	os.Exit(code)
}

func setupTest() (*DuckDBReceiver, error) {
	return NewDBDuckReceiver(testDBPath)
}

func TestUpdateMeasurements_ValidData(t *testing.T) {

	dbr, err := setupTest()
	if err != nil {
		t.Error(err)
	}
	defer dbr.Conn.Close()
	// Call Update Measurements with dummy packet
	msg := getMeasurementEnvelope()
	logMsg := new(string)
	err = dbr.UpdateMeasurements(msg, logMsg)
	time.Sleep(1 * time.Second)
	assert.Nil(t, err, *logMsg)

	// Check if root folder created for database
	if _, err := os.Stat(testDBPath); err != nil {
		assert.False(t, os.IsNotExist(err), "database was not created")
	}

	// verify data was inserted
	rows, err := dbr.Conn.Query("SELECT dbname, metric_name FROM measurements")
	assert.Nil(t, err, "Failed to query database")
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		var dbname, metricName string
		err := rows.Scan(&dbname, &metricName)
		assert.Nil(t, err, "Failed to scan row")
		rowCount++
	}
	assert.Greater(t, rowCount, 0, "No rows found in database")
}
