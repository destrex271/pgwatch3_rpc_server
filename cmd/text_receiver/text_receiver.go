package text_receiver

import (
	"bufio"
	"fmt"
	"log"
	"os"
	. "pgwatch3_rpc_receiver/sinks"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type TextReceiver struct {
	FullPath string
}

func (r *TextReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

	// Write Metrics in a text file
	fileName := fmt.Sprint(msg.CustomTags["pgwatchId"]) + ".txt"
	file, err := os.OpenFile(r.FullPath+"/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		*logMsg = "Unable to open file. Error: " + err.Error()
		log.Println(*logMsg)
		return err
	}

	writer := bufio.NewWriter(file)
	defer file.Close()

	output := "DBName: " + msg.DBName + "\n" + "Metric: " + msg.MetricName + "\n"

	for _, data := range msg.Data {
		output += GetJson(data) + "\n"
	}

	output += "\n===================================\n"

	fmt.Fprintln(writer, output)
	writer.Flush()

	return nil
}
