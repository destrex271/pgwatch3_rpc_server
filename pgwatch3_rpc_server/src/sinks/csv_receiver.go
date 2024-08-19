package sinks

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
)

type CSVReceiver struct {
	FullPath string
}

/*
* Structure for CSV storage:
*   - Database Folder
*       - Metric1.csv
*       - Metric2.csv
 */

var isSyncSignalHandleActive = false

func HandleSyncSignals(recv *Receiver) {
	isSyncSignalHandleActive = true
	// val := recv.GetSyncChannelContent()
	// if val.OPR == "DELETE" {
	// 	fmt.Print("DELETE METRIC: ", val.MetricName)
	// }
	// isSyncSignalHandleActive = false
}

func (r *CSVReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error {

	if len(msg.DBName) == 0 {
		return errors.New("Empty Database")
	}

	// Open/Create Output file
	superFolder := msg.DBName + "-" + fmt.Sprint(msg.CustomTags["pgwatchId"])
	fileName := msg.MetricName + ".csv"
	// if !isSyncSignalHandleActive {
	// 	go HandleSyncSignals(primary_receiver)
	// }

	// Create Database folder if does not exist
	err := os.MkdirAll(r.FullPath+"/"+superFolder, os.ModePerm)

	file, err := os.OpenFile(r.FullPath+"/"+superFolder+"/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	log.Println("[INFO]: Created Folders and Measurement Files")

	if err != nil {
		*logMsg = "Unable to access file. Error: " + err.Error()
		log.Fatal(*logMsg)
		return err
	}

	writer := csv.NewWriter(file)
	log.Println("[INFO]: Adding new measurements for ", msg.DBName)

	for _, data := range msg.Data {
		record := [...]string{
			msg.SourceType,
			msg.MetricName,
			GetJson(data),
			GetJson(msg.CustomTags),
			GetJson(msg.MetricDef),
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
