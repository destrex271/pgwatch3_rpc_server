package sinks

import (
	"context"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const ServerPort = "5050"
const ServerAddress = "0.0.0.0:5050"

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
	go func ()  {
		err := ListenAndServe(receiver, ServerPort)	
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second)

	writer = NewRPCWriter()

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Tests begin from here

func TestGRPCListener(t *testing.T) {
	t.Run("Test Server Connection", func(t *testing.T) {
		msg := testutils.GetTestMeasurementEnvelope()
		req := testutils.GetTestRPCSyncRequest()

		reply, err := writer.client.UpdateMeasurements(context.Background(), msg)
		assert.NoError(t, err, "error calling UpdateMeasurements()")
		assert.Equal(t, reply.GetLogmsg(), "Measurements Updated")

		reply, err = writer.client.SyncMetric(context.Background(), req)
		assert.NoError(t, err, "error calling SyncMetric()")
		assert.Equal(t, reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), "Add"))
	})

	t.Run("Test MsgValidation Interceptor", func(t *testing.T) {
		msg := &pb.MeasurementEnvelope{}

		reply, err := writer.client.UpdateMeasurements(context.Background(), msg)
		assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "empty database name"))
		assert.Nil(t, reply)
	})
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