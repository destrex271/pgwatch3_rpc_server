package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {

	// Important Flags
	// receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text\n\t- parquet")
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	awsEndpoint := flag.String("awsEndpoint", "", "Specify aws endpoint")
	awsRegion := flag.String("awsRegion", "us-east-1", "Specify AWS region")
	username := os.Getenv("awsuser")
	password := os.Getenv("awspasswd")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	var server sinks.Receiver
	server, err := NewS3Receiver(*awsEndpoint, *awsRegion, username, password)
	if err != nil {
		log.Fatal("[ERROR]: Unable to setup receiver")
	}

	rpc.RegisterName("Receiver", server) // Primary Receiver
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Println(err)
	}

	http.Serve(listener, nil)
}
