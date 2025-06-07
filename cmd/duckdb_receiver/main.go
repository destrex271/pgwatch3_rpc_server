package main

import (
	"flag"
	"log"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
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

	server, err := NewDBDuckReceiver(*dbPath, *tableName)
	if err != nil {
		log.Fatal("[ERROR]: Unable to create DuckDB receiver: ", err)
	}

	if err := sinks.Listen(server, *port); err != nil {
		log.Fatal(err)
	}
}
