package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type TextReceiver struct {
	pb.UnimplementedReceiverServer
	FullPath string
}


func NewTextReceiver(fullPath string) (tr *TextReceiver) {
	tr = &TextReceiver{
		FullPath: fullPath,
	}
	return tr
}

func (r *TextReceiver) UpdateMeasurements(ctx context.Context, req *pb.MeasurementEnvelope) (*pb.Reply, error) {
	// Write Metrics in a text file
	fileName := req.DBName + ".txt"
	file, err := os.OpenFile(r.FullPath + "/" + fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		logMsg := "Unable to open file. Error: " + err.Error()
		log.Println(logMsg)
		return &pb.Reply{Logmsg: logMsg}, err
	}

	writer := bufio.NewWriter(file)
	defer func() {_ = file.Close()}()

	output := "DBName: " + req.DBName + "\n" + "Metric: " + req.MetricName + "\n"

	for _, data := range req.Data {
		output += sinks.GetJson(data) + "\n"
	}

	output += "\n===================================\n"

	_, err = fmt.Fprintln(writer, output)
	if err != nil {
		return &pb.Reply{Logmsg: "error updating measurements"}, err
	}
	_ = writer.Flush()

	return &pb.Reply{Logmsg: "Measurements Updated"}, err 
}

func (r *TextReceiver) SyncMetric(ctx context.Context, syncReq *pb.SyncReq) (*pb.Reply, error) {
	// do nothing	
	return nil, nil
}