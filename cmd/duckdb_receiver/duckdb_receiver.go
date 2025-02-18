package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

type DuckDBReceiver struct {
	Ctx  context.Context
	Conn *sql.DB
	sinks.SyncMetricHandler
}

func (dbr *DuckDBReceiver) initializeTable() {
	_, err := dbr.Conn.Exec("CREATE TABLE IF NOT EXISTS measurements(dbname VARCHAR, metric_name VARCHAR, data JSON, custom_tags JSON, metric_def JSON, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (dbname, timestamp))")
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Table successfully created")
}

func NewDBDuckReceiver(databaseName string) (dbr *DuckDBReceiver, err error) {
	db, err := sql.Open("duckdb", databaseName)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	dbr = &DuckDBReceiver{
		Conn:              db,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	dbr.initializeTable()
	return dbr, nil
}

func (r *DuckDBReceiver) InsertMeasurements(data *api.MeasurementEnvelope, ctx context.Context) error {
	fmt.Print("DATA RECIEVED: ", "\n\n\n")
	metricDef, _ := json.Marshal(data.MetricDef)
	fmt.Println(string(metricDef))

	return nil
}

func (r *DuckDBReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

	log.Printf("Received measurement. DBName: '%s', MetricName: '%s', DataPoints: %d",
		msg.DBName, msg.MetricName, len(msg.Data))

	if len(msg.DBName) == 0 {
		*logMsg = "empty database name"
		return errors.New(*logMsg)
	}

	if len(msg.MetricName) == 0 {
		*logMsg = "empty metric name"
		return errors.New(*logMsg)
	}

	if len(msg.Data) == 0 {
		*logMsg = "no measurements"
		return errors.New(*logMsg)
	}

	err := r.InsertMeasurements(msg, context.Background())
	if err != nil {
		*logMsg = err.Error()
		return err
	}

	log.Println("[INFO]: Inserted batch at : " + time.Now().String())
	*logMsg = "[INFO]: Successfully inserted batch!"
	return nil
}
