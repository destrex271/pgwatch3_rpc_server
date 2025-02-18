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
	// TODO: change below "measurements" to dbr.TableName
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
		DBName:            databaseName,
		TableName:         "measurements",
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	dbr.initializeTable()
	return dbr, nil
}

// type MeasurementEnvelope struct {
// 	DBName           string
// 	SourceType       string
// 	MetricName       string
// 	CustomTags       map[string]string
// 	Data             Measurements
// 	MetricDef        Metric
// 	RealDbname       string
// 	SystemIdentifier string
// }

func (r *DuckDBReceiver) InsertMeasurements(data *api.MeasurementEnvelope, ctx context.Context) error {
	fmt.Print("DATA RECIEVED: ", "\n\n\n")
	metricDef, _ := json.Marshal(data.MetricDef)
	fmt.Println(string(metricDef))
	customTagsJSON, _ := json.Marshal(data.CustomTags)
	// measurementJSON, _ := json.Marshal(data.Data)

	// https://duckdb.org/docs/clients/go.html
	// https://github.com/marcboeker/go-duckdb/blob/main/examples/appender/main.go
	// read more on this https://pkg.go.dev/github.com/marcboeker/go-duckdb@v1.8.4#NewConnector
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
