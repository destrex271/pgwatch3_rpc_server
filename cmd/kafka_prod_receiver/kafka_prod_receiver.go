package main

import (
	"context"
	"errors"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/segmentio/kafka-go"
)

type KafkaProdReceiver struct {
	conn      *kafka.Conn
	topic     string
	partition int
	uri       string
	sinks.SyncMetricHandler
}

func NewKafkaProducer(host string, port string, topic string, partition int) (kpr KafkaProdReceiver, err error) {
	var conn *kafka.Conn
	conn, err = kafka.DialLeader(context.Background(), "tcp", host+":"+port, topic, partition)
	if err != nil {
		return
	}

	kpr = KafkaProdReceiver{
		conn:              conn,
		topic:             topic,
		partition:         partition,
		uri:               host + ":" + string(port),
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return kpr, nil
}

func (r KafkaProdReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	if len(msg.DBName) == 0 {
		*logMsg = "Empty Record Delievered"
		return errors.New("Empty Record")
	}

	return nil
}
