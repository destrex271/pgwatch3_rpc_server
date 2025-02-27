package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {

	// Important Flags
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	flag.Parse()

	portInt, error := strconv.Atoi(*port)
	if error != nil || portInt < 0 || portInt > 65535 {
		log.Println("[ERROR]: Invalid Port Number")
		return
	}

	var server sinks.Receiver
	user := os.Getenv("user")
	password := os.Getenv("password")
	serverURI := os.Getenv("server")
	dbname := os.Getenv("dbname")
	log.Println(user, password, serverURI)
	server, err := NewClickHouseReceiver(user, password, dbname, serverURI, false)
	if err != nil {
		log.Fatal("[ERROR]: Unable to create Click house receiver: ", err)
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
