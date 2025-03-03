package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	dbPath := flag.String("dbPath", "metrics.duckdb", "Path to the DuckDB database file")
	tableName := flag.String("tableName", "measurements", "Name of the measurements table")

	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	dbr, err := NewDBDuckReceiver(*dbPath, *tableName)
	if err != nil {
		log.Fatal(err)
	}

	rpc.RegisterName("Receiver", dbr) // Primary Receiver
	log.Println("[INFO]: DuckDB Receiver Initialized with database:", *dbPath)
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)

}
