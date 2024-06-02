package main

import(
    "os"
    "bufio"
    "log"
)

type Measurement struct{
    name string
    description string
}

type Receiver struct{}

func (r *Receiver) UpdateMetrics(measurement Measurement, reply *int){
    // Write Metrics in a text file
    file, err := os.Open("measurements.txt")
    if err != nil{
        log.Fatal("Error: Unable to open measurements file", err)
    }

    writer := bufio.NewWriter(file)

    _, err = writer.Write([]byte("Name:" + measurement.name + "\nDescription: " + measurement.description + "\n+======================================+"))

    if err != nil{
        log.Fatal("Unable to Write to file", err)
    }

    err = writer.Flush()
    
    if err != nil{
        log.Fatal("Unable to Flush data to the file", err)
    }
}
