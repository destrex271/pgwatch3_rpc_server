package main

import (
	"context"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"

	"github.com/parquet-go/parquet-go"
)

type ParquetReceiver struct {
	bufferPath string
	sinks.SyncMetricHandler
}

type ParquetSchema struct {
	DBName            string
	MetricName        string
	Data              string // json string
	Tags              string
}

func NewParquetReceiver(fullPath string) *ParquetReceiver {
	// Create buffer storage
	buffer_path := fullPath + "/parquet_readings"
	_ = os.MkdirAll(buffer_path, os.ModePerm)

	pr := &ParquetReceiver{
		bufferPath: buffer_path,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go pr.HandleSyncMetric()
	return pr
}

func (r ParquetReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return nil, err
	}

	file := r.bufferPath + "/" + msg.DBName + ".parquet"
	if _, err := os.Stat(file); os.IsNotExist(err) {
		_, _ = os.Create(file)
		log.Printf("[INFO]: Created File %s", file)
	}

	_, err := os.Open(file)
	if err != nil {
		log.Println("[ERROR]: Unable to open file", err)
		return nil, err
	}

	data_points, err := parquet.ReadFile[ParquetSchema](file)
	if err != nil {
		data_points = []ParquetSchema{}
	}

	data := ParquetSchema{}
	data.DBName = msg.DBName
	data.MetricName = msg.MetricName
	data.Tags, err = sinks.GetJson(msg.CustomTags)
	if err != nil {
		return nil, err
	}

	for _, measurement := range msg.GetData() {
		data.Data, err = sinks.GetJson(measurement)
		if err != nil {
			return nil, err
		}
		data_points = append(data_points, data)
	}

	err = parquet.WriteFile(file, data_points)
	if err != nil {
		log.Printf("[ERROR]: Unable to write to parquet file %s.", file)
		return nil, err
	}
	log.Println("[INFO]: Updated Measurements for Database: ", msg.DBName)

	return &pb.Reply{}, nil
}