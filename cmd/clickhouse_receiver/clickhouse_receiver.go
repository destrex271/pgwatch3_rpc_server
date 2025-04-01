package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

type ClickHouseReceiver struct {
	Ctx  context.Context
	Conn driver.Conn
	sinks.SyncMetricHandler
	Engine string
}

func GetConnection(user string, password string, dbname string, serverURI string, isTest bool) (driver.Conn, error) {
	dialCount := 0

	var conn driver.Conn
	var err error

	if isTest {
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:     []string{serverURI},
			Protocol: clickhouse.Native,
			Auth: clickhouse.Auth{
				Database: dbname,
				Username: user,
				Password: password,
			},
			DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
				dialCount++
				var d net.Dialer
				return d.DialContext(ctx, "tcp", addr)
			},
			TLS: &tls.Config{},
		})

	} else {
		log.Println("Getting normal connection")
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:     []string{serverURI},
			Protocol: clickhouse.Native,
			Auth: clickhouse.Auth{
				Database: dbname,
				Username: user,
				Password: password,
			},
			TLS: &tls.Config{},
		})
	}

	log.Println(conn)
	if err != nil {
		return nil, err
	}

	err = conn.Ping(context.Background())
	log.Println(err)
	if err != nil {
		return nil, err
	}
	return conn, err
}

func NewClickHouseReceiver(user string, password string, dbname string, serverURI string, isTest bool) (chr *ClickHouseReceiver, err error) {
	// Get clickhouse connection
	conn, err := GetConnection(user, password, dbname, serverURI, isTest)
	if err != nil {
		return nil, err
	}

	// Create new struct
	clickhouseRec := &ClickHouseReceiver{
		Conn:              conn,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
		Engine:            "MergeTree",
	}

	// Setup tables
	log.Println("[INFO]: Setting up Measurements table...")
	err = clickhouseRec.SetupTables(context.Background())
	if err != nil {
		return nil, err
	}

	go clickhouseRec.HandleSyncMetric()

	log.Println("[INFO]: Done!")
	return clickhouseRec, nil
}

// Setup tables
func (r *ClickHouseReceiver) SetupTables(ctx context.Context) error {

	// Try creating JSON type if possible --
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS Measurements(dbname String,custom_tags Map(String, String),metric_def JSON,real_dbname String,system_identifier String,source_type String,data JSON,timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp)) ENGINE=%s`, r.Engine)
	err := r.Conn.Exec(ctx, query)

	if err != nil {
		// String Query
		log.Println("[INFO]: Unable to enforce JSON object. Will use string for storing JSON data")
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS Measurements(dbname String,custom_tags Map(String, String),metric_def String,real_dbname String,system_identifier String,source_type String,data String,timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp)) ENGINE=%s`, r.Engine)
	}

	err = r.Conn.Exec(ctx, query)
	if err != nil {
		return errors.New("failed to create Measurements table: " + err.Error())
	}
	return nil
}

// Insert data
func (r *ClickHouseReceiver) InsertMeasurements(data *api.MeasurementEnvelope, ctx context.Context) error {

	// customTags, _ := json.Marshal(data.CustomTags)
	metricDef, _ := json.Marshal(data.MetricDef)

	batch, err := r.Conn.PrepareBatch(ctx, `
		INSERT INTO Measurements (dbname, custom_tags, metric_def, real_dbname, system_identifier, source_type, data)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	for _, measurement := range data.Data {
		measurementJson, err := json.Marshal(measurement)
		if err != nil {
			msg := "unable to insert measurments"
			log.Println("[ERROR]: " + msg)
			log.Println(msg)
		}

		err = batch.Append(data.DBName, data.CustomTags, string(metricDef), data.RealDbname, data.SystemIdentifier, data.SourceType, string(measurementJson))
		if err != nil {
			msg := "unable to insert data - " + err.Error()
			log.Println("[ERROR]: " + msg)
			return errors.New(msg)
		}
	}
	err = batch.Send()
	if err != nil {
		return err
	}

	return nil
}

func (r *ClickHouseReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

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

func (r *ClickHouseReceiver) HandleSyncMetric() {
	req := <-r.SyncChannel
	log.Println("[INFO]: handle Sync Request", req)
}
