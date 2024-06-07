package main

import (
	"net"
	"net/http"
	"net/rpc"
    "log"
    "flag"
)

func setupRPCServer(){}

func main(){
    
    receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
    flag.Parse()

    if *receiverType == "csv"{
        server := new(CSVReceiver)
        rpc.Register(server)
    }else if *receiverType == "text"{
        // Only for testing
        server := new(TextReceiver)
        rpc.Register(server)
    }else{
        // Throw Error
        log.Fatal("No Sink Type was provided. Please use the --type option")
        return
    }

    rpc.HandleHTTP()

    listener, err := net.Listen("tcp", ":1234")

    if err != nil{
        log.Fatal(err)
    }

    http.Serve(listener, nil)
}
