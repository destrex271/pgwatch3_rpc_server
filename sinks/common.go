package sinks

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	server := grpc.NewServer()
	pb.RegisterReceiverServer(server, receiver)
	log.Println("[INFO]: Registered Receiver")
	// if no error it should never return
	return server.Serve(lis)
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