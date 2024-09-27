package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	// Important Flags
	// receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text\n\t- parquet")
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	serverURI := flag.String("ollamaURI", "http://localhost:11434", "URI for Ollama server")
	pgURI := flag.String("pgURI", "postgres://pgwatch:pgwatchadmin@localhost:5432/postgres", "connection string for postgres")
	batchSize := flag.Int("batchSize", 10, "Specify batch size for generating LLM insights")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	var server sinks.Receiver
	server, err := NewLlamaReceiver(*serverURI, *pgURI, context.Background(), *batchSize)
	if err != nil {
		log.Fatal(err)
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
