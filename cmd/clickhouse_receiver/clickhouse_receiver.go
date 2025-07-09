package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type ClickHouseReceiver struct {
	Conn driver.Conn
	sinks.SyncMetricHandler
	Engine string
}

func GetConnection(User string, Password string, DBName string, serverURI string, isTest bool) (driver.Conn, error) {
	var conn driver.Conn
	var err error
	dialCount := 0

	options := &clickhouse.Options{
		Addr:     []string{serverURI},
		Protocol: clickhouse.Native,
		Auth: clickhouse.Auth{
			Database: DBName,
			Username: User,
			Password: Password,
		},
		TLS: &tls.Config{},
	}

	if isTest {
		options.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	}

	conn, err = clickhouse.Open(options)
	if err != nil {
		return nil, err
	}
	err = conn.Ping(context.Background())
	return conn, err
}

func NewClickHouseReceiver(User string, Password string, DBName string, serverURI string, isTest bool) (*ClickHouseReceiver, error) {
	conn, err := GetConnection(User, Password, DBName, serverURI, isTest)
	if err != nil {
		return nil, err
	}

	chr := &ClickHouseReceiver{
		Conn:              conn,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
		Engine:            "MergeTree",
	}

	err = chr.SetupTables()
	if err != nil {
		return nil, err
	}

	go chr.HandleSyncMetric()
	return chr, nil
}

func (r *ClickHouseReceiver) SetupTables() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS Measurements(dbname String, metric_name String, custom_tags Map(String, String), data JSON, timestamp DateTime DEFAULT now(), PRIMARY KEY (dbname, timestamp)) ENGINE=%s`, r.Engine)
	err := r.Conn.Exec(context.TODO(), query)

	if err != nil {
		log.Println("[INFO]: Unable to enforce JSON object. Will use string for storing Measurements data")
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS Measurements(dbname String, metric_name String, custom_tags Map(String, String), data String, timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp)) ENGINE=%s`, r.Engine)
	}

	err = r.Conn.Exec(context.TODO(), query)
	return err
}

func (r *ClickHouseReceiver) InsertMeasurements(ctx context.Context, data *pb.MeasurementEnvelope) error {
	batch, err := r.Conn.PrepareBatch(ctx, `INSERT INTO Measurements (dbname, metric_name, custom_tags, data) VALUES (?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	for _, measurement := range data.GetData() {
		measurementJson := sinks.GetJson(measurement)
		err = batch.Append(
			data.GetDBName(), 
			data.GetMetricName(),
			data.GetCustomTags(),
			measurementJson,
		)

		if err != nil {
			msg := "unable to insert data - " + err.Error()
			log.Println(msg)
			return errors.New(msg)
		}
	}

	err = batch.Send()
	return err
}

func (r *ClickHouseReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return nil, err
	}

	err := r.InsertMeasurements(ctx, msg)
	if err != nil {
		return nil, err
	}

	log.Println("[INFO]: Inserted batch at : " + time.Now().String())
	return &pb.Reply{}, nil
}