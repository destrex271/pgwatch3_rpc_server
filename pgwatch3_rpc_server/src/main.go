package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (receiver *Receiver) UpdateMeasurements(writeRequest *WriteRequest, status *int) error {
	if receiver.sink_type == CSV {
		writer := new(CSVReceiver)
		writer.UpdateMetrics(writeRequest, status)
	}
	return nil
}

func main() {

	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
	flag.Parse()

	server := new(Receiver)

	if *receiverType == "csv" {
		server.sink_type = CSV
	} else if *receiverType == "text" {
		// Only for testing
		server.sink_type = TEXT
	} else {
		// Throw Error
		server.sink_type = NONE
		log.Fatal("No Sink Type was provided. Please use the --type option")
		return
	}

	rpc.Register(server)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":1234")

	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)
}
