package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

const TEST_DATABASE_NAME string = "pgwatch_test.duckdb"

var testDBPath string

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
	_ = os.Remove(testDBPath)
	_ = os.Remove(testDir)
	os.Exit(code)
}

func setupTest() (*DuckDBReceiver, error) {
	return NewDBDuckReceiver(testDBPath, "measurements")
}

func TestInitialize(t *testing.T) {
	dbr, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	// assert the table creation
	rows, err := dbr.Conn.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='measurements'")
	assert.Nil(t, err, "could not to query tables")
	defer func() {_ = rows.Close()}()
	tableExists := rows.Next()
	assert.True(t, tableExists, "Measurements table was not created")

	// assert the structure
	// note - pk and notnull are not INT types but bool types in duckdb
	columns, err := dbr.Conn.Query("PRAGMA table_info(measurements)")
	assert.Nil(t, err, "Failed to get table information")
	defer func() {_ = columns.Close()}()
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
	requiredColumns := []string{"dbname", "metric_name", "data", "custom_tags", "timestamp"}
	for _, col := range requiredColumns {
		assert.True(t, columnNames[col], fmt.Sprintf("Required column '%s' missing from table", col))
	}
}

func TestUpdateMeasurements(t *testing.T) {
	dbr, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {_ = dbr.Conn.Close()}()

	// Call Update Measurements with dummy data
	msg := GetTestMeasurementEnvelope()
	_, err = dbr.UpdateMeasurements(context.Background(), msg)
	assert.Nil(t, err)

	// Check if root folder created for database
	if _, err := os.Stat(testDBPath); err != nil {
		assert.False(t, os.IsNotExist(err), "database was not created")
	}

	// verify data was inserted
	rows, err := dbr.Conn.Query("SELECT dbname, metric_name FROM measurements")
	assert.Nil(t, err, "Failed to query database")
	defer func(){_ = rows.Close()}()

	rowCount := 0
	for rows.Next() {
		var dbname, metricName string
		err := rows.Scan(&dbname, &metricName)
		assert.Nil(t, err, "Failed to scan row")
		rowCount++
	}
	assert.Greater(t, rowCount, 0, "No rows found in database")
}