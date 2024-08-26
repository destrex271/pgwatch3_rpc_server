package parquet_receiver

import (
	"errors"
	"log"
	"os"
	. "pgwatch3_rpc_receiver/sinks"

	"github.com/parquet-go/parquet-go"
)

type ParqReceiver struct {
	FullPath string
}

type ParquetSchema struct {
	DBName            string
	SourceType        string
	MetricName        string
	Data              string // json string
	Tags              string
	MetricDefinitions string // json string
	SysIdentifier     string
}

func (r *ParqReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error {

	if len(msg.DBName) == 0 {
		*logMsg = "False Record delieverd"
		return errors.New("Empty Database!")
	}

	filename := msg.DBName + ".parquet"

	// Create temporary storage and buffer storage
	buffer_path := r.FullPath + "/buffer_storage"
	os.MkdirAll(buffer_path, os.ModePerm)

	if _, err := os.Stat(buffer_path + "/" + filename); errors.Is(err, os.ErrNotExist) {
		os.Create(buffer_path + "/" + filename)
		log.Println("[INFO]: Created File")
	}

	_, err := os.Open(buffer_path + "/" + filename)
	if err != nil {
		log.Println("[ERROR]: Unable to open file", err)
	}

	data_points, err := parquet.ReadFile[ParquetSchema](buffer_path + "/" + filename)
	if err != nil {
		data_points = []ParquetSchema{}
	}

	log.Println("[INFO]: Updated Measurements for Database: ", msg.DBName)
	for _, metric_data := range msg.Data {
		// populate data
		data := new(ParquetSchema)
		data.DBName = msg.DBName
		data.SourceType = msg.SourceType
		data.MetricName = msg.MetricName
		data.Data = GetJson(metric_data)
		data.MetricDefinitions = GetJson(msg.MetricDef)
		data.Tags = GetJson(msg.CustomTags)
		data.SysIdentifier = msg.SystemIdentifier

		// Append to data points
		data_points = append(data_points, *data)
	}

	err = parquet.WriteFile(buffer_path+"/"+filename, data_points)

	if err != nil {
		log.Fatal("[ERROR]: Unable to write to file.\nStacktrace -> ", err)
	}

	return nil
}
