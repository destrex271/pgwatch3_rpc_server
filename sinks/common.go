package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func GetJson[K map[string]string | map[string]any | float64 | *structpb.Struct | []*structpb.Struct | *pb.MeasurementEnvelope](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}

func ListenAndServe(receiver pb.ReceiverServer, port string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return err
	}
	server := grpc.NewServer(
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

func MsgValidationInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {  
	msg, ok := req.(*pb.MeasurementEnvelope)
	if ok {
		if err := IsValidMeasurement(msg); err != nil {
			return nil, err
		}
	}
    return handler(ctx, req)  
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

func IsValidMeasurement(msg *pb.MeasurementEnvelope) error {
	if msg.GetDBName() == "" {
		return status.Error(codes.InvalidArgument, "empty database name")
	}
	if msg.GetMetricName() == "" {
		return status.Error(codes.InvalidArgument, "empty metric name")
	}
	if len(msg.GetData()) == 0 {
		return status.Error(codes.InvalidArgument, "no data provided")
	}
	return nil
}