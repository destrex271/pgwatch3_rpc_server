package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
)

var dbPath string

func TestMain(m *testing.M) {
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	testDir := filepath.Join(currentDir, "test_tmp")

	err = os.MkdirAll(testDir, 0755)
	if err != nil {
		panic(err)
	}
	dbPath = filepath.Join(testDir, "pgwatch_test.duckdb")

	exitCode := m.Run()
	_ = os.RemoveAll(testDir)
	os.Exit(exitCode)
}

func TestInitialize(t *testing.T) {
	dbr, err := NewDBDuckReceiver(dbPath, "measurements")
	assert.NoError(t, err, "error creating duckdb receiver")

	// assert the table creation
	rows, err := dbr.Conn.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='measurements'")
	assert.NoError(t, err, "could not to query tables")
	defer func() {_ = rows.Close()}()

	tableExists := rows.Next()
	assert.True(t, tableExists, "Measurements table was not created")

	// assert the structure
	// note - pk and notnull are not INT types but bool types in duckdb
	columns, err := dbr.Conn.Query("PRAGMA table_info(measurements)")
	assert.NoError(t, err, "Failed to get table information")
	defer func() {_ = columns.Close()}()

	columnNames := make(map[string]bool)
	for columns.Next() {
		var cid int
		var name, type_name string
		var notnull bool
		var dflt_value interface{}
		var pk bool

		err = columns.Scan(&cid, &name, &type_name, &notnull, &dflt_value, &pk)
		assert.NoError(t, err, "Failed to scan column information")
		columnNames[name] = true
	}

	// assert required columns exist (in this setting.)
	requiredColumns := []string{"dbname", "metric_name", "data", "custom_tags", "timestamp"}
	for _, col := range requiredColumns {
		assert.True(t, columnNames[col], fmt.Sprintf("Required column '%s' missing from table", col))
	}
}

func TestUpdateMeasurements(t *testing.T) {
	dbr, err := NewDBDuckReceiver(dbPath, "measurements")
	assert.NoError(t, err, "error creating duckdb receiver")

	for cnt := range 5 {
		// Call Update Measurements with dummy data
		msg := testutils.GetTestMeasurementEnvelope()
		_, err = dbr.UpdateMeasurements(context.Background(), msg)
		assert.NoError(t, err)

		// verify data was inserted
		rows, err := dbr.Conn.Query("SELECT dbname, metric_name, data, custom_tags FROM measurements")
		assert.NoError(t, err, "Failed to query database")
		defer func(){_ = rows.Close()}()

		rowCount := 0
		for rows.Next() {
			var dbname, metricName, customTags string
			var data map[string]any
			err := rows.Scan(&dbname, &metricName, &data, &customTags)
			assert.NoError(t, err, "Failed to scan row")

			customTagsJSON := sinks.GetJson(msg.GetCustomTags())
			measurement := sinks.GetJson(msg.GetData()[0])

			assert.Equal(t, msg.GetDBName(), dbname)
			assert.Equal(t, msg.GetMetricName(), metricName)
			assert.Equal(t, customTagsJSON, customTags)
			assert.Equal(t, measurement, sinks.GetJson(data))

			rowCount++
		}
		assert.Equalf(t, rowCount, cnt + 1, "Expected %v rows got %v", cnt + 1, rowCount)
	}
}