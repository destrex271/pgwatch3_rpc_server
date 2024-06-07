package main

import (
	"net"
	"net/http"
	"net/rpc"
    "log"
)

func main(){
    // server := new(TextReceiver)
    server := new(CSVReceiver)
    rpc.Register(server)
    rpc.HandleHTTP()

    listener, err := net.Listen("tcp", ":1234")

    if err != nil{
        log.Fatal(err)
    }

    http.Serve(listener, nil)
}
