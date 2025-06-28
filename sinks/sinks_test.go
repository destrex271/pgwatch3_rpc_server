package sinks

import (
	"crypto/tls"
	"crypto/x509"
	"net/rpc"
	"os"
	"testing"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
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

func (w *Writer) Write(username, password string) (string, error) {
	var logMsg string
	if err := w.client.Call("Receiver.UpdateMeasurements", &EnvelopeWrapper{
		RPCServerCreds: RPCServerCreds{
			Username: username,
			Password: password,
		},
	}, &logMsg); err != nil {
		return "", err
	}	
	return logMsg, nil
}

func getTestRPCSyncRequest() *api.RPCSyncRequest {
	return &api.RPCSyncRequest{
		DbName:     "test_database",
		MetricName: "test_metric",
		Operation:  api.AddOp,
	}
}

// Tests begin from here --------------------------------------------------

func TestHTTPListener(t *testing.T) {
	server := NewSink()

	username := "pgwatch"
	password := "pgwatch"
	_ = os.Setenv("RPC_USERNAME", username)
	_ = os.Setenv("RPC_PASSWORD", password)

	go func() {
		err := Listen(server, ServerPort)
		assert.NoError(t, err)
	}()
	time.Sleep(time.Second)

	w := NewRPCWriter(false)
	logMsg, err := w.Write(username, password)
	assert.NoError(t, err)
	assert.Equal(t, "Measurements Updated", logMsg)

	_, err = w.Write(username, "")
	assert.Error(t, err)

	_, err = w.Write("", password)
	assert.Error(t, err)

	_, err = w.Write("", "")
	assert.Error(t, err)
}

func TestTLSListener(t *testing.T) {
	server := NewSink()

	username := "pgwatch"
	password := "pgwatch"
	_ = os.Setenv("RPC_USERNAME", username)
	_ = os.Setenv("RPC_PASSWORD", password)
	_ = os.Setenv("RPC_SERVER_KEY", ServerKey)
	_ = os.Setenv("RPC_SERVER_CERT", ServerCert)

	go func() {
		err := Listen(server, TLSServerPort)
		assert.NoError(t, err)
	}()
	time.Sleep(time.Second)

	tw := NewRPCWriter(true)
	logMsg, err := tw.Write(username, password)
	assert.NoError(t, err)
	assert.Equal(t, "Measurements Updated", logMsg)

	_, err = tw.Write(username, "")
	assert.Error(t, err)

	_, err = tw.Write("", password)
	assert.Error(t, err)

	_, err = tw.Write("", "")
	assert.Error(t, err)
}

func TestNewSyncMetricHandler(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")
}

func TestSyncMetric_ValidData(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.Nil(t, err, "Encoutnered an Error")
}

func TestSyncMetric_InvalidOperation(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.Operation = -1 

	// Send data to Sync Metric Handler and check if it returns any error
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "invalid operation type")
}

func TestSyncMetric_EmptyDatabase(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(0)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.DbName = ""

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "empty database")
}

func TestSyncMetric_EmptyMetric(t *testing.T) {
	chan_len := 1024
	// Get new handler
	handler := NewSyncMetricHandler(chan_len)
	assert.NotNil(t, handler, "Sync Metric Handler is nil")

	// Check if channel is of expected length
	assert.Equal(t, cap(handler.SyncChannel), chan_len, "Channel not of expected length")

	data := getTestRPCSyncRequest()
	data.MetricName = ""

	// Send data to Sync Metric Handler and check if it returns any errosr
	response := new(string)
	err := handler.SyncMetric(data, response)
	assert.EqualError(t, err, "empty metric provided")
}

func TestHandleSyncMetric(t *testing.T) {
	handler := NewSyncMetricHandler(1024)
	// handler routine
	go handler.HandleSyncMetric()

	logMsg := "test msg"
	for range 10 {
		// issue a channel write
		_ = handler.SyncMetric(getTestRPCSyncRequest(), &logMsg)
		time.Sleep(10 * time.Millisecond)
		// Ensure the Channel has been emptied
		assert.Equal(t, len(handler.SyncChannel), 0)
	}
}