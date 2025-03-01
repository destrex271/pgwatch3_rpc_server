package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {

	// Important Flags
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	kafkaHost := flag.String("kafkaHost", "localhost:9092", "Specify the host and port of the kafka instance")
	autoadd := flag.Bool("autoadd", true, "Specifies if new databases are automatically added as a new kafka topic. Default is true. You can disable this service and send an 'ADD' sync metric signal before sending data")
	flag.Parse()

	portInt, error := strconv.Atoi(*port)
	if error != nil || portInt < 0 || portInt > 65535 {
		log.Println("[ERROR]: Invalid Port Number")
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
