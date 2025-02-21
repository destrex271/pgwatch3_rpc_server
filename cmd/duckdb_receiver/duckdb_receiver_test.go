package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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

const TEST_DATABASE_NAME string = "pgwatch_test.duckdb"

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

	testDBPath = filepath.Join(testDir, TEST_DATABASE_NAME) // database path

	// run the tests
	code := m.Run()

	// cleanup
	os.Remove(testDBPath)
	os.Remove(testDir)
	os.Exit(code)
}

func setupTest() (*DuckDBReceiver, error) {
	return NewDBDuckReceiver(testDBPath, "measurements")
}

func TestInitialize(t *testing.T) {
	db, err := sql.Open("duckdb", testDBPath)
	if err != nil {
		t.Error(err)
	}
	dbr := &DuckDBReceiver{
		Conn:              db,
		DBName:            testDBPath,
		TableName:         "measurements",
		Ctx:               context.Background(),
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	dbr.initializeTable()
	createTableQuery := "CREATE TABLE IF NOT EXISTS " + dbr.TableName + "(dbname VARCHAR, metric_name VARCHAR, data JSON, custom_tags JSON, metric_def JSON, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (dbname, timestamp))"
	_, err = dbr.Conn.Exec(createTableQuery)
	if err != nil {
		t.Error(err)
	}

	// assert the table creation
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='measurements'")
	assert.Nil(t, err, "could not to query tables")
	defer rows.Close()
	tableExists := rows.Next()
	assert.True(t, tableExists, "Measurements table was not created")

	// assert the structure
	// note - pk and notnull are not INT types but bool types in duckdb
	columns, err := db.Query("PRAGMA table_info(measurements)")
	assert.Nil(t, err, "Failed to get table information")
	defer columns.Close()
	columnNames := make(map[string]bool)
	for columns.Next() {
		var cid int
		var name, type_name string
		var notnull bool
		var dflt_value interface{}
		var pk bool
		err = columns.Scan(&cid, &name, &type_name, &notnull, &dflt_value, &pk)
		assert.Nil(t, err, "Failed to scan column information")
		columnNames[name] = true
	}
	// assert required columns exist (in this setting.)
	requiredColumns := []string{"dbname", "metric_name", "data", "custom_tags", "metric_def", "timestamp"}
	for _, col := range requiredColumns {
		assert.True(t, columnNames[col], fmt.Sprintf("Required column '%s' missing from table", col))
	}
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
	// time.Sleep(1 * time.Second)
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

func TestUpdateMeasurements_EMPTY_DBNAME(t *testing.T) {
	dbr, err := setupTest()
	if err != nil {
		t.Error(err)
	}
	defer dbr.Conn.Close()

	msg := getMeasurementEnvelope()
	msg.DBName = ""

	logMsg := new(string)
	err = dbr.UpdateMeasurements(msg, logMsg)
	assert.NotNil(t, err, "Expected error when measurement DBName is empty, but got nil")
	if err != nil {
		assert.Contains(t, err.Error(), "empty database name", "Error message should mention empty database name")
	}
}

func TestUpdateMeasurements_EMPTY_METRICNAME(t *testing.T) {
	dbr, err := setupTest()
	if err != nil {
		t.Error(err)
	}
	defer dbr.Conn.Close()

	msg := getMeasurementEnvelope()
	msg.MetricName = ""

	logMsg := new(string)
	err = dbr.UpdateMeasurements(msg, logMsg)
	assert.NotNil(t, err, "Expected error when measurement MetricName is empty, but got nil")
	if err != nil {
		assert.Contains(t, err.Error(), "empty metric name", "Error message should mention empty metric name")
	}
}

func TestUpdateMeasurements_EMPTY_DATA(t *testing.T) {
	dbr, err := setupTest()
	if err != nil {
		t.Error(err)
	}
	defer dbr.Conn.Close()

	msg := getMeasurementEnvelope()
	msg.Data = []map[string]any{}

	logMsg := new(string)
	err = dbr.UpdateMeasurements(msg, logMsg)
	assert.NotNil(t, err, "Expected error when measurement Data is empty, but got nil")
	if err != nil {
		assert.Contains(t, err.Error(), "no measurements", "Error message should mention no measurements")
	}
}
