package csv_receiver

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	. "pgwatch3_rpc_receiver/sinks"
)

func main() {

	// Important Flags
	port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	var server Receiver
	syncHandler := new(SyncMetricHandler)

	log.Println("[INFO]: CSV Receiver Intialized")
	server = &CSVReceiver{FullPath: *StorageFolder}

	rpc.RegisterName("Receiver", server)     // Primary Receiver
	rpc.RegisterName("Handler", syncHandler) // Sync Metric Handler
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)

	if err != nil {
		log.Println(err)
	}

	http.Serve(listener, nil)
}
