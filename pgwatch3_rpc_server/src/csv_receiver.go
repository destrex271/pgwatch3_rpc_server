package main

import (
	"log"
	"os"
    "fmt"
    "encoding/csv"
)

type CSVReceiver struct{}

/*
* Structure for CSV storage:
*   - Database Folder
*       - Metric1.csv
*       - Metric2.csv
*/

func (r *CSVReceiver) UpdateMeasurements(writeRequest *WriteRequest, reply *int, fullPath string) error{
    // Open/Create Output file
    superFolder := writeRequest.Msg.DBName + "-" + fmt.Sprint(writeRequest.PgwatchID)
    fileName := writeRequest.Msg.MetricName + ".csv" 

    // Create Database folder if does not exist
    err := os.MkdirAll(fullPath + "/" + superFolder, os.ModePerm)

    file, err := os.OpenFile(fullPath + "/" + superFolder + "/" + fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)

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
