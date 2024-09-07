package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/segmentio/kafka-go"
)

type KafkaProdReceiver struct {
	conn_regisrty map[string]*kafka.Conn
	uri           string
	auto_add      bool
	sinks.SyncMetricHandler
}

func (r *KafkaProdReceiver) HandleSyncMetric() {
	req := <-r.SyncChannel
	switch req.Operation {
	case "DELETE":
		r.CloseConnectionForDB(req.DbName)
	case "ADD":
		r.AddTopicIfNotExists(req.DbName)
	}
}

func NewKafkaProducer(host string, topics []string, partitions []int, auto_add bool) (kpr *KafkaProdReceiver, err error) {
	connRegistry := make(map[string]*kafka.Conn)
	partitions_len := len(partitions)
	for index, topic := range topics {
		var conn *kafka.Conn
		if partitions_len > 0 {
			conn, err = kafka.DialLeader(context.Background(), "tcp", host, topic, partitions[index])
		} else {
			conn, err = kafka.DialLeader(context.Background(), "tcp", host, topic, 0)
		}
		if err != nil {
			return
		}

		connRegistry[topic] = conn
	}
	kpr = &KafkaProdReceiver{
		conn_regisrty:     connRegistry,
		uri:               host,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
		auto_add:          auto_add,
	}
	// Start sync Handler routine
	go kpr.HandleSyncMetric()

	return kpr, nil
}

func (r *KafkaProdReceiver) AddTopicIfNotExists(dbName string) error {
	new_conn, err := kafka.DialLeader(context.Background(), "tcp", r.uri, dbName, 0)
	if err != nil {
		return err
	}

	r.conn_regisrty[dbName] = new_conn
	log.Println("[INFO]: Added Database " + dbName + " to sink")
	return nil
}

func (r *KafkaProdReceiver) CloseConnectionForDB(dbName string) error {
	err := r.conn_regisrty[dbName].Close()

	if err != nil {
		return err
	}

	delete(r.conn_regisrty, dbName)
	log.Println("[INFO]: Deleted Database " + dbName + " from sink")
	return nil
}

func (r *KafkaProdReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	// Kafka Recv
	if len(msg.DBName) == 0 {
		*logMsg = "Empty Database"
		return errors.New(*logMsg)
	}

	if len(msg.MetricName) == 0 {
		*logMsg = "Empty Metric Name"
		return errors.New(*logMsg)
	}

	// Get connection for database topic
	conn := r.conn_regisrty[msg.DBName]

	if conn == nil {
		log.Println("[WARNING]: Connection does not exist for database " + msg.DBName)
		if r.auto_add {
			log.Println("[INFO]: Adding database " + msg.DBName + " since Auto Add is enabled. You can disable it by restarting the sink with autoadd option as false")
			err := r.AddTopicIfNotExists(msg.DBName)
			if err != nil {
				log.Println("[ERROR]: Unable to create new connection")
				return err
			}
			conn = r.conn_regisrty[msg.DBName]
		} else {
			return errors.New("[FATAL] Auto Add not enabled. Please restart the sink with autoadd=true")
		}
	}

	// Convert MeasurementEnvelope struct to json and write it as message in kafka
	json_data, err := json.Marshal(msg)
	if err != nil {
		*logMsg = "Unable to convert measurements data to json"
		return err
	}

	// conn.SetWriteDeadline(time.Now())
	_, err = conn.WriteMessages(
		kafka.Message{Value: json_data},
	)

	if err != nil {
		*logMsg = "Failed to write messages!"
		return err
	}

	log.Println("[INFO]: Measurements Written to topic - ", msg.DBName)

	return nil
}
