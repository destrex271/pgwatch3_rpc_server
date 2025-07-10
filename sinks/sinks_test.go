package sinks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/rpc"
	"os"
	"testing"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const RootCA = "./rpc_tests_certs/ca.crt"
const ServerCert = "./rpc_tests_certs/server.crt"
const ServerKey = "./rpc_tests_certs/server.key"
const ServerPort = "5050"
const ServerAddress = "localhost:5050"
const TLSServerPort = "6060"
const TLSServerAddress = "localhost:6060"

type Sink struct {
	SyncMetricHandler
}

func (s *Sink) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	*logMsg = "Measurements Updated"
	return nil
}

func NewSink() *Sink {
	return &Sink{
		SyncMetricHandler: NewSyncMetricHandler(1024),
	}
}

type Writer struct {
	client *rpc.Client
}

func NewRPCWriter(TLS bool) *Writer {
	if !TLS {
		client, err := rpc.DialHTTP("tcp", ServerAddress)
		if err != nil {
			panic(err)
		}
		return &Writer{client: client}
	}

	ca, err := os.ReadFile(RootCA)
	if err != nil {
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(ca)
	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	conn, err := tls.Dial("tcp", TLSServerAddress, tlsConfig)
	if err != nil {
		panic(err)
	}
	return &Writer{client: rpc.NewClient(conn)}
}

func (w *Writer) Write() string {
	var logMsg string
	if err := w.client.Call("Receiver.UpdateMeasurements", &api.MeasurementEnvelope{}, &logMsg); err != nil {
		panic(err)
	}	
	return logMsg
}

func GetTestRPCSyncRequest() *pb.SyncReq {
	return &pb.SyncReq{
		DBName:     "test_database",
		MetricName: "test_metric",
		Operation:  pb.SyncOp_AddOp,
	}
}

// Tests begin from here --------------------------------------------------

func TestHTTPListener(t *testing.T) {
	server := NewSink()
	go func() {
		_ = Listen(server, ServerPort)
	}()
	time.Sleep(time.Second)

	w := NewRPCWriter(false)
	logMsg := w.Write()
	assert.Equal(t, "Measurements Updated", logMsg)
}

func TestTLSListener(t *testing.T) {
	server := NewSink()
	_ = os.Setenv("RPC_SERVER_KEY", ServerKey)
	_ = os.Setenv("RPC_SERVER_CERT", ServerCert)
	go func() {
		_ = Listen(server, TLSServerPort)
	}()
	time.Sleep(time.Second)

	tw := NewRPCWriter(true)
	logMsg := tw.Write()
	assert.Equal(t, "Measurements Updated", logMsg)
}

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
		_, _ = handler.SyncMetric(context.Background(), GetTestRPCSyncRequest())
		time.Sleep(10 * time.Millisecond)
		// Ensure the Channel has been emptied
		assert.Empty(t, len(handler.syncChannel))
	}
}

func TestInvalidMeasurement(t *testing.T) {
	msg := &api.MeasurementEnvelope{
		DBName: "",
	}
	err := IsValidMeasurement(msg)
	assert.EqualError(t, err, "empty database name")

	msg = &api.MeasurementEnvelope{
		DBName: "dummy",
		MetricName: "",
	}
	err = IsValidMeasurement(msg)
	assert.EqualError(t, err, "empty metric name")

	msg = &api.MeasurementEnvelope{
		DBName: "dummy",
		MetricName: "dummy",
	}
	err = IsValidMeasurement(msg)
	assert.EqualError(t, err, "no data provided")
}