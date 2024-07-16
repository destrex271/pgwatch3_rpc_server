package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

type TextReceiver struct{}

func (r *TextReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error {
	// Write Metrics in a text file
	fileName := fmt.Sprint(msg.CustomTags["pgwatchId"]) + ".txt"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		*logMsg = "Unable to open file. Error: " + err.Error()
		log.Fatal(*logMsg)
		return err
	}

	writer := bufio.NewWriter(file)
	defer file.Close()

	output := "DBName: " + msg.DBName + "\n" + "Metric: " + msg.MetricName + "\n======================================\n"

	fmt.Fprintln(writer, output)
	writer.Flush()

	return nil
}
