package sinks

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func ListenAndServe(receiver pb.ReceiverServer, port string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return err
	}

	creds := LoadTLSCredentials()
	server := grpc.NewServer(
		grpc.Creds(creds),
		grpc.ChainUnaryInterceptor(
			AuthInterceptor,
			MsgValidationInterceptor,
		),
	)

	pb.RegisterReceiverServer(server, receiver)
	log.Println("[INFO]: Registered Receiver")
	// if no error it should never return
	return server.Serve(lis)
}

var SERVER_USERNAME = os.Getenv("PGWATCH_RPC_SERVER_USERNAME")
var SERVER_PASSWORD = os.Getenv("PGWATCH_RPC_SERVER_PASSWORD")

func AuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	authenticated := true

	if ok && SERVER_USERNAME != "" {
		clientUsername := md.Get("username")[0]
		authenticated = (clientUsername == SERVER_USERNAME)
	}

	if ok && SERVER_PASSWORD != "" {
		clientPassword := md.Get("password")[0]
		authenticated = (clientPassword == SERVER_PASSWORD) && authenticated
	}

	if !authenticated {
		return nil, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	return handler(ctx, req)
}

var SERVER_CERT = os.Getenv("PGWATCH_RPC_SERVER_CERT")
var SERVER_KEY  = os.Getenv("PGWATCH_RPC_SERVER_KEY")

func LoadTLSCredentials() credentials.TransportCredentials {
	cert, err := tls.LoadX509KeyPair(SERVER_CERT, SERVER_KEY)
	if err != nil {
		// results in grpc.Creds(nil) => ignoring encryption
		return nil
	}

	log.Println("Valid cert/key pair detected - enabling TLS")
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return credentials.NewTLS(tlsConfig)
}

func MsgValidationInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {  
	msg, ok := req.(*pb.MeasurementEnvelope)
	if ok {
		if err := IsValidMeasurement(msg); err != nil {
			return nil, err
		}
	}
    return handler(ctx, req)  
}