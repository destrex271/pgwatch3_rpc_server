package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

type TextReceiver struct{}

func (r *TextReceiver) UpdateMetrics(measurement *MeasurementMessage, status *int) error{
    // Write Metrics in a text file
    fileName := "measurements.txt"
    file, err := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)

    if err != nil{
        *status = -1
        log.Fatal("Unable to open file. Error: ", err)
        return err
    }

    writer := bufio.NewWriter(file)
    defer file.Close()

    output := "DBName: " + measurement.DBName + "\n" + "Metric: " + measurement.MetricName + "\n======================================\n"

    fmt.Fprintln(writer, output)
    writer.Flush()

    return nil
}
