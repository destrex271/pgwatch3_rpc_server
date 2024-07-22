package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
    . "pgwatch3_rpc_receiver/sinks"
)

func main() {

	// Important Flags
	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	storage_folder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Fatal("[ERROR]: No Port Specified")
		return
	}

	log.Println("Setting up Server.....")
	server := new(Receiver)

	server.SyncChannel = make(chan SyncReq, 10)
	if *receiverType == "csv" {
		server.SinkType = CSV
		server.StorageFolder = *storage_folder
	} else if *receiverType == "text" {
		// Only for testing
		server.SinkType = TEXT
	} else {
		// Throw Error
		server.SinkType = NONE
		log.Fatal("[ERROR]: No Sink Type was provided. Please use the --type option")
		return
	}

	rpc.Register(server)
	log.Println("RPC registered")
	rpc.HandleHTTP()

	log.Println("listening...")
	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	log.Println("Found -> ", listener)
	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)
}
