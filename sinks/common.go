package sinks

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

func GetJson[K map[string]string | map[string]any | float64 | api.MeasurementEnvelope | api.Metric](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}

func Listen(server Receiver, port string) {
	rpc.RegisterName("Receiver", server) // Primary Receiver
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))

	if err != nil {
		log.Fatal(err)
	}

	log.Println("[INFO]: Registered Receiver")
	http.Serve(listener, nil)
}