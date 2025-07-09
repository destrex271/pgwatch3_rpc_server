package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type DuckDBReceiver struct {
	Ctx       context.Context
	Conn      *sql.DB
	DBName    string
	TableName string
	sinks.SyncMetricHandler
}

func (dbr *DuckDBReceiver) initializeTable() {
	// Allow only alphanumeric and underscores in table names
	validateTableName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !validateTableName.MatchString(dbr.TableName) {
		log.Fatal("Invalid table name: potential SQL injection risk")
	}

	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (dbname VARCHAR, metric_name VARCHAR, data JSON, custom_tags JSON, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (dbname, timestamp))`, dbr.TableName)

	_, err := dbr.Conn.Exec(createTableQuery)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Table successfully created")
}

func NewDBDuckReceiver(databaseName string, tableName string) (dbr *DuckDBReceiver, err error) {
	// close fatally if table isnt created, or if receiver isnt initailized properly
	db, err := sql.Open("duckdb", databaseName)
	if err != nil {
		log.Fatal(err)
	}

	dbr = &DuckDBReceiver{
		Conn:      db,
		DBName:    databaseName,
		TableName: tableName,
		Ctx:       context.Background(),
	}

	dbr.initializeTable()
	go dbr.HandleSyncMetric()

	return dbr, nil
}
func (r *DuckDBReceiver) InsertMeasurements(ctx context.Context, data *pb.MeasurementEnvelope) error {
	customTagsJSON, _ := json.Marshal(data.GetCustomTags())

	// use direct SQL approach - just use the existing connection with the standard insert statement
	tx, err := r.Conn.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO " + r.TableName +
		" (dbname, metric_name, data, custom_tags, timestamp) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		log.Printf("error from preparing statement: %v", err)
		_ = tx.Rollback()
		return err
	}
	defer func() {_ = stmt.Close()}()

	for _, measurement := range data.GetData() {
		measurementJSON, err := sinks.GetJson(measurement)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		_, err = stmt.Exec(
			data.GetDBName(),
			data.GetMetricName(),
			measurementJSON,
			customTagsJSON,
			time.Now(),
		)

		if err != nil {
			log.Printf("error from insert: %v", err)
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("error from committing transaction: %v", err)
		return err
	}
	return nil
}

func (r *DuckDBReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return nil, err
	}

	log.Printf("Received measurement. DBName: '%s', MetricName: '%s', DataPoints: %d",
		msg.GetDBName(), msg.GetMetricName(), len(msg.GetData()))

	err := r.InsertMeasurements(ctx, msg)
	if err != nil {
		return nil, err
	}

	log.Println("[INFO]: Inserted batch at : " + time.Now().String())
	return &pb.Reply{}, nil
}