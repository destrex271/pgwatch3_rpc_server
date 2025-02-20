package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/marcboeker/go-duckdb"
)

type DuckDBReceiver struct {
	Ctx       context.Context
	Conn      *sql.DB
	DBName    string
	TableName string
	sinks.SyncMetricHandler
}

func (dbr *DuckDBReceiver) initializeTable() {
	createTableQuery := "CREATE TABLE IF NOT EXISTS " + dbr.TableName + "(dbname VARCHAR, metric_name VARCHAR, data JSON, custom_tags JSON, metric_def JSON, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (dbname, timestamp))"
	_, err := dbr.Conn.Exec(createTableQuery)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Table successfully created")
}

func NewDBDuckReceiver(databaseName string) (dbr *DuckDBReceiver, err error) {
	// close fatally if table isnt created, or if receiver isnt initailized properly
	db, err := sql.Open("duckdb", databaseName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	dbr = &DuckDBReceiver{
		Conn:              db,
		DBName:            databaseName,
		TableName:         "measurements",
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	dbr.initializeTable()
	return dbr, nil
}

func (r *DuckDBReceiver) InsertMeasurements(data *api.MeasurementEnvelope, ctx context.Context) error {
	metricDef, _ := json.Marshal(data.MetricDef)
	customTagsJSON, _ := json.Marshal(data.CustomTags)

	// https://github.com/marcboeker/go-duckdb/blob/main/examples/appender/main.go
	connector, err := duckdb.NewConnector(r.DBName, nil)
	if err != nil {
		log.Print("Error: ", err)
		return err
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		log.Print("Error: ", err)
		return err
	}
	defer conn.Close()
	// appender is used for  bulk inserts and already uses the transaction context. see- https://duckdb.org/docs/clients/go.html
	appender, err := duckdb.NewAppenderFromConn(conn, "", r.TableName)
	if err != nil {
		log.Print("Error: ", err)
		return err
	}
	defer appender.Close()
	for _, measurement := range data.Data {
		measurementJSON, err := json.Marshal(measurement)
		if err != nil {
			log.Print("failed to marshal measurement: ", err)
			return err
		}

		if err := appender.AppendRow(
			data.DBName,
			data.MetricName,
			string(measurementJSON),
			string(customTagsJSON),
			string(metricDef),
			time.Now(),
		); err != nil {
			log.Print("could not append row: ", err)
			return err
		}
	}
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
