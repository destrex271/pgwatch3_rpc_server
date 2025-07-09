package main

import (
	"flag"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	user := os.Getenv("user")
	password := os.Getenv("password")
	serverURI := os.Getenv("server")
	dbname := os.Getenv("dbname")
	server, err := NewClickHouseReceiver(user, password, dbname, serverURI, false)
	if err != nil {
		log.Fatal("[ERROR]: Unable to create Click house receiver: ", err)
	}

	if err = sinks.ListenAndServe(server, *port); err != nil {
		log.Fatal(err)
	}
}
