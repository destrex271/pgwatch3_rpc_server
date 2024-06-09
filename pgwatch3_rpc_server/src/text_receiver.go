package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

type TextReceiver struct{}

func (r *TextReceiver) UpdateMeasurements(writeRequest *WriteRequest, status *int) error{
    // Write Metrics in a text file
    fileName := writeRequest.FileName + "_" + fmt.Sprint(writeRequest.PgwatchID) + ".txt"
    file, err := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)

    if err != nil{
        *status = -1
        log.Fatal("Unable to open file. Error: ", err)
        return err
    }

    writer := bufio.NewWriter(file)
    defer file.Close()

    output := "DBName: " + writeRequest.Msg.DBName + "\n" + "Metric: " + writeRequest.Msg.MetricName + "\n======================================\n"

    fmt.Fprintln(writer, output)
    writer.Flush()

    return nil
}
