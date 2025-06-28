package sinks

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
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

func Listen(server Receiver, port string) error {
	authWrapper := NewAuthWrapper(server)
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Receiver", authWrapper); err != nil {
		return err
	}

	ServerCrtPath := os.Getenv("RPC_SERVER_CERT")
	ServerKeyPath := os.Getenv("RPC_SERVER_KEY")

	if ServerCrtPath  == "" || ServerKeyPath == "" {
		// Listen Without TLS
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
		if err != nil {
			return err
		}
		log.Println("[INFO]: Registered Receiver")
		return http.Serve(listener, nil)
	}

	// Listen With TLS
	cert, err := tls.LoadX509KeyPair(ServerCrtPath, ServerKeyPath)
	if err != nil {
		return fmt.Errorf("[ERROR]: error loading server certificates: %s", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port), tlsConfig)
	if err != nil {
		return err
	}
	log.Println("[INFO]: Registered Receiver with TLS")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}