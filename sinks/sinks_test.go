package sinks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSyncMetricHandler_ValidSyncReqs(t *testing.T) {
	chan_len := 1024
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")
	assert.Equal(t, cap(handler.syncChannel), chan_len, "Channel not of expected length")

	validReqs := map[string]*pb.SyncReq{
		"non-empty AddOp": {DBName: "test", MetricName: "test", Operation: pb.SyncOp_AddOp},
		"non-empty DeleteOp": {DBName: "test", MetricName: "test", Operation: pb.SyncOp_DeleteOp},
		"empty MetricName AddOp": {DBName: "test", MetricName: "", Operation: pb.SyncOp_AddOp},
		"empty MetricName DeleteOp": {DBName: "test", MetricName: "", Operation: pb.SyncOp_DeleteOp},
	}

	for name, req := range validReqs {
		t.Run(name, func(t *testing.T) {
			opName := "Add"
			if req.GetOperation() == pb.SyncOp_DeleteOp {
				opName = "Delete"
			}

			reply, err := handler.SyncMetric(context.Background(), req)
			assert.NoError(t, err)
			assert.Equal(t, reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), opName))
		})
	}
}

func TestSyncMetricHandler_InValidSyncReqs(t *testing.T) {
	chan_len := 1024
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")
	assert.Equal(t, cap(handler.syncChannel), chan_len, "Channel not of expected length")

	invalidReqs := map[string]*pb.SyncReq{
		"empty DBName AddOp": {DBName: "", MetricName: "test", Operation: pb.SyncOp_AddOp},
		"empty DBName DeleteOp": {DBName: "", MetricName: "test", Operation: pb.SyncOp_DeleteOp},
		"empty DBName and MetricName AddOp": {DBName: "", MetricName: "", Operation: pb.SyncOp_AddOp},
		"empty DBName and MetricName DeleteOp": {DBName: "", MetricName: "", Operation: pb.SyncOp_DeleteOp},

		"non-empty InvalidOp": {DBName: "test", MetricName: "test", Operation: pb.SyncOp_InvalidOp},
		"empty DBName InvalidOp": {DBName: "", MetricName: "test", Operation: pb.SyncOp_InvalidOp},
		"empty MetricName InvalidOp": {DBName: "test", MetricName: "", Operation: pb.SyncOp_InvalidOp},
		"empty DBName and MetricName InvalidOp": {DBName: "", MetricName: "", Operation: pb.SyncOp_InvalidOp},
	}

	for name, req := range invalidReqs {
		t.Run(name, func(t *testing.T) {
			errMsg := "invalid sync request DBName can't be empty"
			if req.GetOperation() == pb.SyncOp_InvalidOp {
				errMsg = "invalid operation type"
			}

			reply, err := handler.SyncMetric(context.Background(), req)
			assert.EqualError(t, status.Error(codes.InvalidArgument, errMsg), err.Error())
			assert.Empty(t, reply.GetLogmsg())
		})
	}
}

func TestDefaultHandleSyncMetric(t *testing.T) {
	handler := NewSyncMetricHandler(1024)
	// handler routine
	go handler.HandleSyncMetric()

	for range 10 {
		// issue a channel write
		_, _ = handler.SyncMetric(context.Background(), testutils.GetTestRPCSyncRequest())
		time.Sleep(10 * time.Millisecond)
		// Ensure the Channel has been emptied
		assert.Empty(t, len(handler.syncChannel))
	}
}

func TestIsValidMeasurement(t *testing.T) {
	msg := &pb.MeasurementEnvelope{}
	err := IsValidMeasurement(msg)
	assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "empty database name"))

	msg = &pb.MeasurementEnvelope{
		DBName: "dummy",
	}
	err = IsValidMeasurement(msg)
	assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "empty metric name"))

	msg = &pb.MeasurementEnvelope{
		DBName: "dummy",
		MetricName: "dummy",
	}
	err = IsValidMeasurement(msg)
	assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "no data provided"))

	msg = testutils.GetTestMeasurementEnvelope()
	err = IsValidMeasurement(msg)
	assert.NoError(t, err)
}