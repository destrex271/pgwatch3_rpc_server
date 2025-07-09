package main

import (
	"flag"
	"log"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	kafkaHost := flag.String("kafkaHost", "localhost:9092", "Specify the host and port of the kafka instance")
	autoadd := flag.Bool("autoadd", true, "Specifies if new databases are automatically added as a new kafka topic. Default is true. You can disable this service and send an 'ADD' sync metric signal before sending data")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	server, err := NewKafkaProducer(*kafkaHost, nil, nil, *autoadd)
	if err != nil {
		log.Println("[ERROR]: Unable to create Kafka Producer ", err)
	}

	if err := sinks.ListenAndServe(server, *port); err != nil {
		log.Fatal(err)
	}	
}
