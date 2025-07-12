package main

import (
	"flag"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
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

	server, err := NewS3Receiver(*awsEndpoint, *awsRegion, username, password)
	if err != nil {
		log.Fatal("[ERROR]: Unable to create S3 receiver", err)
	}

	if err := sinks.ListenAndServe(server, *port); err != nil {
		log.Fatal(err)
	}
}
