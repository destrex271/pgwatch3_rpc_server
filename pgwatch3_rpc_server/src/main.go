package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	. "pgwatch3_rpc_receiver/examples"
	. "pgwatch3_rpc_receiver/sinks"
)

func StorageError() {
	log.Default().Fatal("[ERROR]: No storage location was specified to store metric files")
}

func main() {

	// Important Flags
	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text\n\t- parquet")
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	var server Receiver
	syncHandler := new(SyncMetricHandler)

	if *receiverType == "csv" {
		log.Println("[INFO]: CSV Receiver Intialized")
		server = &CSVReceiver{FullPath: *StorageFolder}
	} else if *receiverType == "text" {
		server = &TextReceiver{FullPath: *StorageFolder}
	} else if *receiverType == "parquet" {
		server = &ParqReceiver{FullPath: *StorageFolder}
	}

	rpc.RegisterName("Receiver", server)     // Primary Receiver
	rpc.RegisterName("Handler", syncHandler) // Sync Metric Handler
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Println(err)
	}

	http.Serve(listener, nil)
}
