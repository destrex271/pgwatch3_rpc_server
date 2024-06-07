package main

import (
	"log"
	"os"
	"strings"
    "fmt"
    "encoding/csv"
)

type CSVReceiver struct{}

func (r *CSVReceiver) UpdateMetrics(writeRequest *WriteRequest, reply *int) error{
    // Open/Create Output file
    fileName := strings.Split(writeRequest.FileName, ".")[0] + "_" + fmt.Sprint(writeRequest.PgwatchID) + ".csv" 
    file, err := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)

    if err != nil{
        log.Fatal("Unable to access file. Error: ", err)
        return err
    }

    writer := csv.NewWriter(file)

    record := [...]string{
        writeRequest.Msg.DBName,
        writeRequest.Msg.SourceType,
        writeRequest.Msg.MetricName,
        "CustomTags",
        // TODO: This is a map, need to setup a different way to write this field, writeRequest.Msg.CustomTags,
        "Measurements",
        // TODO: Measurements Object, need to setup a way to retrieve string version, Data            
        "Metrics",
        // TODO: Metric field, need to setup a way to retrieve as string, MetricDef        
        writeRequest.Msg.RealDbname,
        writeRequest.Msg.SystemIdentifier,
    }

    // Writing measurements to CSV
    if err := writer.Write(record[:]); err != nil{
        log.Fatal("Unable to write to CSV file " + fileName + "Error: " + err.Error())
        return err
    }

    writer.Flush()

    if err := writer.Error(); err != nil{
        log.Fatal("Error: ", err)
        return err
    }

    return nil
}
