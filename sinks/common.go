package sinks

import (
	"encoding/json"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GetJson(value any) (string, error) {
	jsonString, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(jsonString), nil
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