package csv_receiver

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
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

func (r *CSVReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	if len(msg.DBName) == 0 {
		return errors.New("Empty Database")
	}

	// Open/Create Output file
	superFolder := msg.DBName + "-" + fmt.Sprint(msg.CustomTags["pgwatchId"])
	fileName := msg.MetricName + ".csv"

	// Create Database folder if does not exist
	err := os.MkdirAll(r.FullPath+"/"+superFolder, os.ModePerm)
	if err != nil {
		return err
	}

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
			sinks.GetJson(data),
			sinks.GetJson(msg.CustomTags),
			sinks.GetJson(msg.MetricDef),
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
