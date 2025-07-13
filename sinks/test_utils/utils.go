package testutils

import (
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/protobuf/types/known/structpb"
)

func GetTestMeasurementEnvelope() *pb.MeasurementEnvelope {
	st, err := structpb.NewStruct(map[string]any{"key": "val"})
	if err != nil {
		panic(err)
	}
	measurements := []*structpb.Struct{st}
	return &pb.MeasurementEnvelope{
		DBName:           "test",
		MetricName:       "testMetric",
		CustomTags: 	  map[string]string{"tagName": "tagValue"},
		Data:             measurements,
	}
}

func GetTestRPCSyncRequest() *pb.SyncReq {
	return &pb.SyncReq{
		DBName:     "test_database",
		MetricName: "test_metric",
		Operation:  pb.SyncOp_AddOp,
	}
}