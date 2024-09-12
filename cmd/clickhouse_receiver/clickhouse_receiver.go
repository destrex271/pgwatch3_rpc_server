package main

import (
	"context"
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

func GetConnection(user string, password string, dbname string, serverURI string) (driver.Conn, error) {
	dialCount := 0
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{serverURI},
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
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         5,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Duration(10) * time.Minute,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "k.d-app", Version: "0.0.1"},
			},
		},
	})

	log.Println(err)
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

func NewClickHouseReceiver(user string, password string, dbname string, serverURI string) (chr *ClickHouseReceiver, err error) {
	// Get clickhouse connection
	conn, err := GetConnection(user, password, dbname, serverURI)
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
	clickhouseRec.SetupTables(context.Background())
	log.Println("[INFO]: Done!")
	return clickhouseRec, nil
}

// Setup tables
func (r *ClickHouseReceiver) SetupTables(ctx context.Context) error {

	// Create Measurements Table
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS Measurements(dbname String,custom_tags Map(String, String),metric_def String,real_dbname String,system_identifier String,source_type String,data String,timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp)) ENGINE=%s`, r.Engine)
	err := r.Conn.Exec(ctx, query, r.Engine)
	if err != nil {
		return errors.New("failed to create Measurements table: " + err.Error())
	}

	return nil
}

// Insert data
func (r *ClickHouseReceiver) InsertMeasurements(data *api.MeasurementEnvelope, ctx context.Context) error {

	customTags, _ := json.Marshal(data.CustomTags)
	metricDef, _ := json.Marshal(data.MetricDef)

	for _, measurement := range data.Data {
		measurementJson, err := json.Marshal(measurement)
		if err != nil {
			msg := "unable to insert measurments"
			log.Println("[ERROR]: " + msg)
			log.Println(msg)
		}

		query := fmt.Sprintf(`
			INSERT INTO Measurements(dbname, custom_tags, metric_def, real_dbname, system_identifier, source_type, data)
			VALUES ('%s', %s, '%s', '%s', '%s', '%s', '%s');
		`, data.DBName, customTags, metricDef, data.RealDbname, data.SystemIdentifier, data.SourceType, measurementJson)

		err = r.Conn.Exec(ctx, query)
		if err != nil {
			msg := "unable to insert data - " + err.Error()
			log.Println("[ERROR]: " + msg)
			return errors.New(msg)
		}
	}

	return nil
}

func (r *ClickHouseReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	err := r.InsertMeasurements(msg, context.Background())
	if err != nil {
		*logMsg = err.Error()
		return err
	}
	return nil
}
