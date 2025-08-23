package main

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type PubsubReceiver struct {
	client *pubsub.Client
	publisher *pubsub.Publisher
	sinks.SyncMetricHandler
}

func NewPubsubReceiver(projectID string) (*PubsubReceiver, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	topicName := fmt.Sprintf("projects/%s/topics/pgwatch", projectID)
	topic, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicName,
	})
	if err != nil {
		return nil, err
	}

	publisher := client.Publisher(topic.GetName())
	pr := &PubsubReceiver{
		client: client,
		publisher: publisher,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go pr.HandleSyncMetric()
	return pr, nil
}

func (r *PubsubReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	_ = r.publisher.Publish(ctx, &pubsub.Message{Data: data})
	return &pb.Reply{Logmsg: "Message published."}, nil
}