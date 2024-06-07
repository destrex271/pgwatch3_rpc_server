package main

import(
    "os"
    "bufio"
    "log"
)

type CSVReceiver struct{}

func (r *CSVReceiver) UpdateMetrics(measurement MeasurementMessage, reply *int){
    // Open/Create Output file

    // Writing measurements to CSV
}
