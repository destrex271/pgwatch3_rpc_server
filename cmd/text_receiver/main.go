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
	// receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text\n\t- parquet")
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	portInt, err := strconv.Atoi(*port)
	if err != nil || portInt < 0 || portInt > 65535 {
		log.Println("[ERROR]: Invalid Port Number")
		return
	}

	var server sinks.Receiver
	server = TextReceiver{FullPath: *StorageFolder, SyncMetricHandler: sinks.NewSyncMetricHandler(1024)}

	rpc.RegisterName("Receiver", server) // Primary Receiver
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Println(err)
	}

	http.Serve(listener, nil)
}
