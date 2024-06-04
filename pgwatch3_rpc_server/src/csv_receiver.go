package main

import(
    "os"
    "bufio"
    "log"
)

type CSVReceiver struct{}

func (r *CSVReceiver) UpdateMetrics(measurement MeasurementMessage, reply *int){
    // Write Metrics in a text file
    file, err := os.Open("measurements.txt")
    if err != nil{
        log.Fatal("Error: Unable to open measurements file", err)
    }

    writer := bufio.NewWriter(file)

    _, err = writer.Write([]byte("Name:" + measurement.DBName + "\nDescription: " + measurement.MetricName + "\n+======================================+"))

    if err != nil{
        log.Fatal("Unable to Write to file", err)
    }

    err = writer.Flush()
    
    if err != nil{
        log.Fatal("Unable to Flush data to the file", err)
    }
}
