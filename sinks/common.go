package sinks

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type Options struct {
    Port int `short:"p" long:"port" description:"Port number to listen on" required:"true"`
	ServerCrtPath string `long:"server-cert-path" description:"Server TLS Certificate path" env:"SERVER_CERT" required:"true"`
	ServerKeyPath string `long:"server-key-path" description:"Server TLS Private key file path" env:"SERVER_KEY" required:"true"`
}

func Listen(server Receiver, opts *Options) (err error) {
	if err = rpc.RegisterName("Receiver", server); err != nil {
		return 
	}

	cert, err := tls.LoadX509KeyPair(opts.ServerCrtPath, opts.ServerKeyPath)
	if err != nil {
		return 
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", opts.Port), tlsConfig)
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

func GetJson[K map[string]string | map[string]any | float64 | api.MeasurementEnvelope | api.Metric](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}