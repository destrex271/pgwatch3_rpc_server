package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

type ClickHouseReceiver struct {
	Ctx  context.Context
	Conn *sql.DB
	sinks.SyncMetricHandler
	Engine string
}

func GetConnection(user string, password string, serverURI string) (*sql.DB, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{serverURI}, // 9440 is a secure native TCP port
		Protocol: clickhouse.Native,
		TLS:      &tls.Config{}, // enable secure TLS
		Auth: clickhouse.Auth{
			Username: user,
			Password: password,
		},
	})

	if conn != nil {
		return nil, errors.New("Unable to create new connection")
	}

	// row := conn.QueryRow("SELECT 1")
	// var col uint8
	// if err := row.Scan(&col); err != nil {
	// 	fmt.Printf("An error while reading the data: %s", err)
	// } else {
	// 	fmt.Printf("Result: %d", col)
	// }
	return conn, nil
}

func NewClickHouseReceiver(user string, password string, serverURI string) (chr *ClickHouseReceiver, err error) {
	// Get clickhouse connection
	conn, err := GetConnection(user, password, serverURI)
	if err != nil {
		return nil, err
	}

	// Create new struct
	clickhouseRec := &ClickHouseReceiver{
		Conn:              conn,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}
	return clickhouseRec, nil
}

// Setup tables
func (r *ClickHouseReceiver) SetupTables() error {

	// Create Measurements Table
	_, err := r.Conn.Exec(`
		CREATE TABLE IF NOT EXISTS Measurements(
			dbname String,
			custom_tags Map(String, String),
			metric_def JSON,
			real_dbname String,
			system_identifier String,
			source_type String,
			data JSON,
			timestamp DateTime DEFAULT now(),
			PRIMARY KEY (dbname, timestamp)
		) ENGINE=` + r.Engine)
	if err != nil {
		return errors.New("failed to create Measurements table: " + err.Error())
	}

	return nil
}

// Insert data
func (r *ClickHouseReceiver) InsertMeasurements(data *api.MeasurementEnvelope) error {

	customTags, _ := json.Marshal(data.CustomTags)
	metricDef, _ := json.Marshal(data.MetricDef)

	for _, measurement := range data.Data {
		measurementJson, err := json.Marshal(measurement)
		if err != nil {
			msg := "unable to insert measurments"
			log.Println("[ERROR]: " + msg)
			log.Println(msg)
		}

		query := `
			INSERT INTO Measurements(dbname, custom_tags, metric_def, real_dbname, system_identifier, source_type, data)
			VALUES (?, ?, ?, ?, ?, ?, ?);
		`

		_, err = r.Conn.Exec(query, data.DBName, customTags, metricDef, data.RealDbname, data.SystemIdentifier, data.SourceType, measurementJson)
		if err != nil {
			msg := "unable to insert data"
			log.Println("[ERROR]: " + msg)
			return errors.New(msg)
		}
	}

	return nil
}

func (r *ClickHouseReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	err := r.InsertMeasurements(msg)
	if err != nil {
		*logMsg = err.Error()
		return err
	}
	return nil
}
