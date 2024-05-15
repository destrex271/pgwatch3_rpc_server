package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type Args struct{}

type MetricArgs struct {
    name string
    age int32
}

type TimeServer int64

type Metric struct{
    time int64
    age int32
    name string
}

func (t *TimeServer) GiveServerTime(args *Args, reply *int64) error{
    *reply = time.Now().Unix()
    return nil
}

// Return status string on call after updating metric
func (metric *Metric) UpdateMetric(args *MetricArgs, reply *string) error{
    if len(args.name) == 0{
        *reply = "Name length is nil"
        return errors.New("Invalid Name")
    }
    metric.age = args.age
    metric.name = args.name
    *reply = "Success"
    return nil
}

func main(){
    timeserver := new(TimeServer)
    metrics := new(Metric)
    rpc.Register(timeserver)
    rpc.Register(metrics)
    rpc.HandleHTTP()

    listener, err := net.Listen("tcp", ":1234")

    if err != nil{
        log.Fatal("error: ", err)
    }

    http.Serve(listener, nil)
}
