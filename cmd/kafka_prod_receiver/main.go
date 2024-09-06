package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {

	// Important Flags
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	kafkaHost := flag.String("kafkaHost", "localhost:9092", "Specify the host and port of the kafka instance")
	autoadd := flag.Bool("autoadd", true, "Specifies if new databases are automatically added as a new kafka topic. Default is true. You can disable this service and send an 'ADD' sync metric signal before sending data")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	var server sinks.Receiver
	server, err := NewKafkaProducer(*kafkaHost, nil, nil, *autoadd)
	if err != nil {
		log.Println("[ERROR]: Unable to create Kafka Producer ", err)
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
