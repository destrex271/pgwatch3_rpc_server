package sinks

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
    "encoding/json"
)

type CSVReceiver struct{}

/*
* Structure for CSV storage:
*   - Database Folder
*       - Metric1.csv
*       - Metric2.csv
 */

var isSyncSignalHandleActive = false

func HandleSyncSignals(recv *Receiver) {
	isSyncSignalHandleActive = true
	val := recv.GetSyncChannelContent()
	if val.OPR == "DELETE" {
		fmt.Print("DELETE METRIC: ", val.MetricName)
	}
	isSyncSignalHandleActive = false
}

func (r *CSVReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string, fullPath string, primary_receiver *Receiver) error {
	// Open/Create Output file
	superFolder := msg.DBName + "-" + fmt.Sprint(msg.CustomTags["pgwatchId"])
	fileName := msg.MetricName + ".csv"
	if !isSyncSignalHandleActive {
		go HandleSyncSignals(primary_receiver)
	}

	// Create Database folder if does not exist
	err := os.MkdirAll(fullPath+"/"+superFolder, os.ModePerm)

	file, err := os.OpenFile(fullPath+"/"+superFolder+"/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		*logMsg = "Unable to access file. Error: " + err.Error()
		log.Fatal(*logMsg)
		return err
	}

	writer := csv.NewWriter(file)
    // Add column Names
    rec := [...]string{
        "DBName", "SourceType", "MetricName", "Measurements", "CustomTags", "Metric Definitions", 
    }

    if err := writer.Write(rec[:]); err != nil {
        log.Fatal("[ERROR]: Unable to write to CSV file " + fileName + "Error: " + err.Error())
        return err
    }

    for _, data := range msg.Data{
        record := [...]string{
            msg.DBName,
            msg.SourceType,
            msg.MetricName,
            createKeyValuePairs(data),
            createKeyValuePairs_String(msg.CustomTags),
            getJson(msg.MetricDef),
        }

        // Writing measurements to CSV
        if err := writer.Write(record[:]); err != nil {
            log.Fatal("Unable to write to CSV file " + fileName + "Error: " + err.Error())
            return err
        }

        writer.Flush()

        if err := writer.Error(); err != nil {
            log.Fatal("Error: ", err)
            return err
        }
    }

	return nil
}

func createKeyValuePairs(m map[string]any) string {
    jsonString, err := json.Marshal(m)
    if err != nil {
        log.Default().Fatal("Error converting map to JSON string:", err)
    }
    return string(jsonString)
}


func createKeyValuePairs_String(m map[string]string) string {
    jsonString, err := json.Marshal(m)
    if err != nil {
        log.Default().Fatal("[ERROR]: Converting map to JSON string:", err)
    }
    return string(jsonString)
}

func getJson(m Metric) string {
    jsonString, err := json.Marshal(m)
    if err != nil{
        log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
    }
    return string(jsonString)
}
