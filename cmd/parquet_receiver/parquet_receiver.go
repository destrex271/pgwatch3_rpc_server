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
	dbFilePath := r.bufferPath + "/" + msg.GetDBName() + ".parquet"

	data_points, err := parquet.ReadFile[ParquetSchema](dbFilePath)
	if err != nil {
		data_points = []ParquetSchema{}
	}

	data := ParquetSchema{}
	data.DBName = msg.GetDBName()
	data.MetricName = msg.GetMetricName()
	data.Tags, err = sinks.GetJson(msg.CustomTags)
	if err != nil {
		return nil, err
	}

	for _, measurement := range msg.GetData() {
		data.Data, err = sinks.GetJson(measurement)
		if err != nil {
			continue
		}
		data_points = append(data_points, data)
	}

	err = parquet.WriteFile(dbFilePath, data_points)
	if err != nil {
		log.Printf("[ERROR]: Unable to write to parquet file %s.", dbFilePath)
		return nil, err
	}
	log.Println("[INFO]: Updated Measurements for Database: ", msg.GetDBName())

	return &pb.Reply{}, nil
}