package sinks

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

func GetJson[K map[string]string | map[string]any | float64 | api.MeasurementEnvelope | api.Metric](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}

func Listen(server Receiver, port string) (err error) {
	if err = rpc.RegisterName("Receiver", server); err != nil {
		return 
	}

	ServerCrtPath := os.Getenv("SERVER_CERT")
	ServerKeyPath := os.Getenv("SERVER_KEY")

	cert, err := tls.LoadX509KeyPair(ServerCrtPath, ServerKeyPath)
	if err != nil {
		return 
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port), tlsConfig)
	if err != nil {
		return 
	}
	log.Println("[INFO]: Registered Receiver")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}