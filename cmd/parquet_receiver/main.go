package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")

	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	server := NewParquetReceiver(*StorageFolder)

	rpc.RegisterName("Receiver", server)
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)
}
