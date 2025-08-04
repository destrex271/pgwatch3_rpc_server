package main

import (
	"flag"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	// Here users should parse any necessary server and 
	// storage-backend flags or env vars (e.g. port number, database uri) 
	port := flag.String("port", "9999", "Description.")
	some_important_arg := flag.String("arg", "default-value", "Description.")
	flag.Parse()
	sink_required_env := os.Getenv("MY_REQUIRED_ENV")

	// maybe do some checks on them
	if sink_required_env == "some value" {
		log.Fatal("invalid value for `MY_REQUIRED_ENV`")
	}

	// instantiate new receiver object with parsed args 
	server := NewReceiver(sink_required_env, some_important_arg)
	// invoke pre-defined `sinks.ListenAndServe()` to start the server 
	// passing receiver object to use and port number to listen on
	if err := sinks.ListenAndServe(server, *port); err != nil {
		log.Fatal(err)
	}
}