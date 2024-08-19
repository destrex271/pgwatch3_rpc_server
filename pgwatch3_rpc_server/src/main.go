package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	. "pgwatch3_rpc_receiver/sinks"
)

func StorageError() {
	log.Default().Fatal("[ERROR]: No storage location was specified to store metric files")
}

func main() {

	// Important Flags
	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Fatal("[ERROR]: No Port Specified")
		return
	}

	var server Receiver
	if *receiverType == "csv" {
		server = &CSVReceiver{FullPath: *StorageFolder}
	}

	log.Println("{:?}", receiverType)

	rpc.RegisterName("Receiver", server)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	log.Println("Found -> ", listener)

	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)
}
