package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func AuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {  
	username := os.Getenv("RPC_USERNAME")
	password := os.Getenv("RPC_PASSWD")
	md, _ := metadata.FromIncomingContext(ctx)
	if md["username"][0] != username || md["password"][0] != password {
		return nil, fmt.Errorf("unauthorized")
	}

    resp, err := handler(ctx, req)  
    return resp, err  
}

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}

	lis, _ := net.Listen("tcp", ":" + *port)

	server := grpc.NewServer(
		grpc.UnaryInterceptor(AuthInterceptor),
	)
	sinks.RegisterReceiverServer(server, NewTextReceiver(*StorageFolder))

	log.Println("[INFO]: Registered Receiver")
	server.Serve(lis)
}
