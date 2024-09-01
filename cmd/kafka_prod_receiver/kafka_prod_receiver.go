package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

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

func NewKafkaProducer(host string, topic string, partition int) (kpr KafkaProdReceiver, err error) {
	var conn *kafka.Conn
	conn, err = kafka.DialLeader(context.Background(), "tcp", host, topic, partition)
	if err != nil {
		return
	}

	kpr = KafkaProdReceiver{
		conn:              conn,
		topic:             topic,
		partition:         partition,
		uri:               host,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return kpr, nil
}

func (r KafkaProdReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	// Kafka Recv
	if len(msg.DBName) == 0 {
		*logMsg = "Empty Record Delievered"
		return errors.New("Empty Record")
	}

	// Convert MeasurementEnvelope struct to json and write it as message in kafka
	json_data, err := json.Marshal(msg)
	if err != nil {
		*logMsg = "Unable to convert measurements data to json"
		return err
	}

	r.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = r.conn.WriteMessages(
		kafka.Message{Value: json_data},
	)

	if err != nil {
		*logMsg = "Failed to write messages!"
		return err
	}

	log.Println("[INFO]: Measurements Written to topic - ", r.topic)

	return nil
}
