package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcpubsub "github.com/testcontainers/testcontainers-go/modules/gcloud/pubsub"
	"github.com/testcontainers/testcontainers-go/wait"
)

var pubsubContainer *tcpubsub.Container

func TestMain(m *testing.M) {
	var err error
	pubsubContainer, err = tcpubsub.Run(
		context.Background(),
		"gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators",
		tcpubsub.WithProjectID("pubsub-receiver-test-project"),
		testcontainers.WithExposedPorts("8085:8085/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server started"),
		),
	)
	if err != nil {
		panic(err)
	}

	err = os.Setenv("PUBSUB_EMULATOR_HOST", pubsubContainer.URI())
	if err != nil {
		panic(err)
	}

	exitCode := m.Run()	

	if err := testcontainers.TerminateContainer(pubsubContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
	os.Exit(exitCode)
}

func TestPubsubReceiver(t *testing.T) {
	a := assert.New(t)

	psr, err := NewPubsubReceiver(pubsubContainer.ProjectID())
	a.NoError(err)
	a.NotNil(psr)

	t.Run("Test Pub/Sub Receiver UpdateMeasurements()", func(t *testing.T) {
		msg := testutils.GetTestMeasurementEnvelope()
		reply, err := psr.UpdateMeasurements(context.Background(), msg)

		a.NoError(err)
		a.Equal(reply.GetLogmsg(), "Message published.")

		// Try read the published message from the Pub/Sub server.
		sub, err := CreateSubscription(psr)
		a.NoError(err)

		ctx, cancel := context.WithCancel(context.Background())
		err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			var recvd_msg map[string]any
			err := json.Unmarshal(m.Data, &recvd_msg)
			a.NoError(err)

			a.Equal(msg.GetDBName(), recvd_msg["DBName"])
			a.Equal(msg.GetMetricName(), recvd_msg["MetricName"])

			data := recvd_msg["Data"].([]any)
			for i, item := range msg.GetData() {
				recvd_item := data[i].(map[string]any)
				a.Equal(item.AsMap(), recvd_item)
			}

			m.Ack()
			// cancel the ctx to force Receive() to return
			cancel()
		})
		a.NoError(err)
	})

	t.Run("Test calling SyncMetric() from Pub/Sub Receiver", func(t *testing.T) {
		req := testutils.GetTestRPCSyncRequest()
		reply, err := psr.SyncMetric(context.Background(), req)
		a.NoError(err)
		a.Equal(reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), "Add"))
	})
}

func CreateSubscription(psr *PubsubReceiver) (*pubsub.Subscriber, error){
	subName := fmt.Sprintf("projects/%s/subscriptions/test-sub", pubsubContainer.ProjectID())
	topicName := fmt.Sprintf("projects/%s/topics/pgwatch", pubsubContainer.ProjectID())

	subscription, err := psr.client.SubscriptionAdminClient.CreateSubscription(context.Background(),
		&pubsubpb.Subscription{
			Name: subName,
			Topic: topicName,
		},
	)
	if err != nil {
		return nil, err
	}

	sub := psr.client.Subscriber(subscription.GetName())
	return sub, nil
}