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

func (r *CSVReceiver) UpdateMeasurements(msg *MeasurementMessage, reply *int, fullPath string, primary_receiver *Receiver) error{
    // Open/Create Output file
    superFolder := msg.DBName + "-" + fmt.Sprint(msg.CustomTags["pgwatchId"])
    fileName := msg.MetricName + ".csv" 

    val := primary_receiver.GetSyncChannelContent()
    fmt.Println("SIGNAL" + val.DBName + " " + val.OPR)

    // Create Database folder if does not exist
    err := os.MkdirAll(fullPath + "/" + superFolder, os.ModePerm)

    file, err := os.OpenFile(fullPath + "/" + superFolder + "/" + fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)

    if err != nil{
        log.Fatal("Unable to access file. Error: ", err)
        return err
    }


    writer := csv.NewWriter(file)

    record := [...]string{
        msg.DBName,
        msg.SourceType,
        msg.MetricName,
        "CustomTags",
        // TODO: This is a map, need to setup a different way to write this field, msg.CustomTags,
        "Measurements",
        // TODO: Measurements Object, need to setup a way to retrieve string version, Data            
        "Metrics",
        // TODO: Metric field, need to setup a way to retrieve as string, MetricDef        
        msg.RealDbname,
        msg.SystemIdentifier,
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
