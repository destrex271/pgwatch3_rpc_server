package main

import(
    "net/rpc"
    "log"
    "fmt"
)

func main(){
    measurements := new(MeasurementMessage)
    measurements.DBName = "Test"
    measurements.MetricName = "Age"
    client, err := rpc.DialHTTP("tcp", "localhost"+":1234")

    if err != nil{
        log.Fatal("error:", err)
    }

    var status int 
    err = client.Call("TextReceiver.UpdateMetrics", &measurements, &status)
    if err != nil{
        log.Fatal(err)
    }
    fmt.Println("Returned with status: ", status)
}
