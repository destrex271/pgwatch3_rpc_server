package kafka_prod_receiver

import (
	"context"
	"errors"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/segmentio/kafka-go"
)

type KafkaProdReceiver struct {
	conn *kafka.Conn
    topic string
    partition int
    uri string
}

func NewKafkaProducer(host string, port string, topic string, partition int) (*KafkaProdReceiver, error) {
    conn, err := kafka.DialLeader(context.Background(), "tcp", host + ":" + port, topic, partition)
    if err != nil{
        return nil, err
    }

    producer := &KafkaProdReceiver{
        conn: conn,
        topic: topic,
        partition: partition,
        uri: host + ":" + string(port),
    }

    return producer, nil
}

func (r *KafkaProdReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	if len(msg.DBName) == 0 {
		*logMsg = "Empty Record Delievered"
		return errors.New("Empty Record")
	}

	return nil
}
