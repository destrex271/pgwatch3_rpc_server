package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	port := flag.String("port", "", "Port number for the server to listen on.")
	addrsString := flag.String("addrs", "https://localhost:9200", "A comma separated list of Elasticsearch nodes to use.")
	username := flag.String("user", "elastic", "Username for HTTP Basic Authentication.")
	cacertPath := flag.String("ca-file", "./http_ca.crt", "Certificate Authority file path.")
	flag.Parse()
	password := os.Getenv("ELASTIC_PASSWORD")

	addrsList := strings.Split(*addrsString, ",")

	server, err := NewESReceiver(addrsList, *username, password, *cacertPath)
	if err != nil {
		log.Fatal(err)
	}

	if err := sinks.ListenAndServe(server, *port); err != nil {
		log.Fatal(err)
	}
}