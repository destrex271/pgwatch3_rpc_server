package sinks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const PlainServerPort = "5050"
const PlainServerAddress = "localhost:5050" // CN in test cert is `localhost`
const TLSServerPort = "6060"
const TLSServerAddress = "localhost:6060"

const TestRootCA = "./rpc_tests_certs/ca.crt"
const TestCert = "./rpc_tests_certs/server.crt"
const TestPrivateKey = "./rpc_tests_certs/server.key"

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

func NewRPCWriter(withTLS bool) *Writer {
	var creds credentials.TransportCredentials
	var address string

	if withTLS {
		ca, err := os.ReadFile(TestRootCA)
		if err != nil {
			panic(err)
		}

		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(ca)
		tlsClientConfig := &tls.Config{
			RootCAs: certPool,
		}

		creds = credentials.NewTLS(tlsClientConfig)
		address = TLSServerAddress
	} else {
		creds = insecure.NewCredentials()
		address = PlainServerAddress
	}

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		panic(err)
	}

	client := pb.NewReceiverClient(conn)
	return &Writer{
		client: client,
	}
}

func (w *Writer) WriteWithCreds(msg *pb.MeasurementEnvelope, username, password string) (*pb.Reply, error) {
	md := metadata.Pairs(
		"username", username,
		"password", password,
	)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	reply, err := w.client.UpdateMeasurements(ctx, msg)	
	return reply, err
}

var writer *Writer

func TestMain(m *testing.M) {
	receiver := NewSink()
	go func () {
		err := ListenAndServe(receiver, PlainServerPort)	
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	SERVER_CERT = TestCert
	SERVER_KEY = TestPrivateKey
	go func () {
		err := ListenAndServe(receiver, TLSServerPort)	
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	writer = NewRPCWriter(false)

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Tests begin from here

func Test_gRPCServer(t *testing.T) {
	msg := testutils.GetTestMeasurementEnvelope()
	req := testutils.GetTestRPCSyncRequest()

	TLSWriter := NewRPCWriter(true)
	writers := [2]*Writer{writer, TLSWriter}

	for _, w := range writers {
		reply, err := w.client.UpdateMeasurements(context.Background(), msg)
		assert.NoError(t, err, "error calling UpdateMeasurements()")
		assert.Equal(t, reply.GetLogmsg(), "Measurements Updated")

		reply, err = w.client.SyncMetric(context.Background(), req)
		assert.NoError(t, err, "error calling SyncMetric()")
		assert.Equal(t, fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), "Add"), reply.GetLogmsg()) 
	}	
}

func TestMsgValidationInterceptor(t *testing.T) {
	msg := &pb.MeasurementEnvelope{}

	reply, err := writer.client.UpdateMeasurements(context.Background(), msg)
	assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "empty database name"))
	assert.Nil(t, reply)
}

func TestAuthInterceptor(t *testing.T) {
	msg := testutils.GetTestMeasurementEnvelope()

	serverCreds := []string{
		",", 
		"username,password",
		"username,", 
		",password",
	}

	validCreds := [][]string{
		{",", "username,password", "username,", ",password"},
		{"username,password"},
		{"username,", "username,random_password"},
		{",password", "random_username,password"},
	}

	invalidCreds := [][]string{
		{},
		{",password", "username,", "notusername,password", "username,notpassword", "notusername,notpassword"},
		{"notusername,", "notusername,random_password"},
		{",notpassword", "random_username,notpassword"},
	}

	for i, serverCred := range serverCreds {
		SERVER_USERNAME, SERVER_PASSWORD, _ = strings.Cut(serverCred, ",")

		for _, validCred := range validCreds[i] {
			clientUsername, clientPassword, _ := strings.Cut(validCred, ",")
			reply, err := writer.WriteWithCreds(msg, clientUsername, clientPassword)
			assert.NoError(t, err)
			assert.Equal(t, reply.GetLogmsg(), "Measurements Updated")
		}


		for _, invalidCred := range invalidCreds[i] {
			clientUsername, clientPassword, _ := strings.Cut(invalidCred, ",")
			reply, err := writer.WriteWithCreds(msg, clientUsername, clientPassword)
			assert.Error(t, err, status.Error(codes.Unauthenticated, "invalid username or password"))
			assert.Nil(t, reply)
		}
	}
}