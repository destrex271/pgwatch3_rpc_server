package sinks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const ServerPort = "5050"
const ServerAddress = "localhost:5050"

type Sink struct {
	SyncMetricHandler
}

func (s *Sink) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	return &pb.Reply{Logmsg: "Measurements Updated"}, nil
}

func NewSink() *Sink {
	return &Sink{
		SyncMetricHandler: NewSyncMetricHandler(1024),
	}
}

type Writer struct {
	client pb.ReceiverClient
}

func NewRPCWriter() *Writer {
	conn, err := grpc.NewClient(ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))	
	if err != nil {
		panic(err)
	}
	client := pb.NewReceiverClient(conn)
	return &Writer{client: client}
}

func (w *Writer) Write() (string, error) {
	reply, err := w.client.UpdateMeasurements(context.Background(), &pb.MeasurementEnvelope{})
	return reply.GetLogmsg(), err
}

func GetTestRPCSyncRequest() *pb.SyncReq {
	return &pb.SyncReq{
		DBName:     "test_database",
		MetricName: "test_metric",
		Operation:  pb.SyncOp_AddOp,
	}
}

// Tests begin from here --------------------------------------------------

func TestGRPCListener(t *testing.T) {
	server := NewSink()
	go func() {
		err := ListenAndServe(server, ServerPort)
		assert.NoError(t, err)
	}()
	time.Sleep(time.Second)

	w := NewRPCWriter()
	logMsg, err := w.Write()
	assert.NoError(t, err)
	assert.Equal(t, "Measurements Updated", logMsg)
}

func TestSyncMetricHandler(t *testing.T) {
	chan_len := 1024
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")
	assert.Equal(t, cap(handler.syncChannel), chan_len, "Channel not of expected length")

	t.Run("Valid Data", func(t *testing.T) {
		data := GetTestRPCSyncRequest()
		reply, err := handler.SyncMetric(context.Background(), data)
		assert.NoError(t, err)
		assert.Equal(t, reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", data.GetDBName(), data.GetMetricName(), "Add"))

		data.DBName = ""
		reply, err = handler.SyncMetric(context.Background(), data)
		assert.NoError(t, err)
		assert.Equal(t, reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", data.GetDBName(), data.GetMetricName(), "Add"))

		data.DBName = "dummy"
		data.MetricName = ""
		data.Operation = pb.SyncOp_DeleteOp
		reply, err = handler.SyncMetric(context.Background(), data)
		assert.NoError(t, err)
		assert.Equal(t, reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", data.GetDBName(), data.GetMetricName(), "Delete"))
	})

	t.Run("Invalid Operation", func(t *testing.T) {
		data := GetTestRPCSyncRequest()
		data.Operation = pb.SyncOp_InvalidOp

		reply, err := handler.SyncMetric(context.Background(), data)
		assert.Error(t, err)
		assert.Empty(t, reply.GetLogmsg())
	})

	t.Run("Invalid Data", func(t *testing.T) {
		data := GetTestRPCSyncRequest()
		data.DBName = ""
		data.MetricName = ""

		reply, err := handler.SyncMetric(context.Background(), data)
		assert.Error(t, err)
		assert.Empty(t, reply.GetLogmsg())
	})
}

func TestDefaultHandleSyncMetric(t *testing.T) {
	handler := NewSyncMetricHandler(1024)
	// handler routine
	go handler.HandleSyncMetric()

	for range 10 {
		// issue a channel write
		_, _ = handler.SyncMetric(context.Background(), GetTestRPCSyncRequest())
		time.Sleep(10 * time.Millisecond)
		// Ensure the Channel has been emptied
		assert.Empty(t, len(handler.syncChannel))
	}
}

func TestInvalidMeasurement(t *testing.T) {
	msg := &pb.MeasurementEnvelope{}
	err := IsValidMeasurement(msg)
	assert.Error(t, err)

	msg.DBName = "dummy"
	err = IsValidMeasurement(msg)
	assert.Error(t, err)

	msg.MetricName = "dummy"
	err = IsValidMeasurement(msg)
	assert.Error(t, err)
}