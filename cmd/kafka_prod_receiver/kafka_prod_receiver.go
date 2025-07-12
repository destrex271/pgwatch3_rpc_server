package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KafkaProdReceiver struct {
	conn_regisrty map[string]*kafka.Conn
	uri           string
	auto_add      bool
	sinks.SyncMetricHandler
}

// Handle Sync Metric Instructions
func (r *KafkaProdReceiver) HandleSyncMetric() {
	for {
		req, ok := r.GetSyncChannelContent()
		if !ok {
			// channel has been closed
			return
		}

		var err error
		switch req.Operation {
		case pb.SyncOp_AddOp:
			err = r.CloseConnectionForDB(req.GetDBName())
		case pb.SyncOp_DeleteOp:
			err = r.AddTopicIfNotExists(req.GetDBName())
		}

		if err != nil {
			log.Printf("[ERROR] error handling Kafka SyncMetric operation: %s", err)
		}
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

func (r *KafkaProdReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	// Get connection for database topic
	DBName := msg.GetDBName()
	conn, ok := r.conn_regisrty[DBName]
	if !ok {
		log.Println("[WARNING]: Connection does not exist for database " + DBName)
		if r.auto_add {
			log.Println("[INFO]: Adding database " + DBName + " since Auto Add is enabled. You can disable it by restarting the sink with autoadd option as false")
			err := r.AddTopicIfNotExists(DBName)
			if err != nil {
				log.Println("[ERROR]: Unable to create new connection")
				return nil, err
			}
			conn = r.conn_regisrty[DBName]
		} else {
			return nil, status.Error(codes.FailedPrecondition, "auto add not enabled. please restart the sink with autoadd=true")
		}
	}

	// Convert MeasurementEnvelope struct to json and write it as message in kafka
	json_data, err := json.Marshal(msg)
	if err != nil {
		log.Println("Unable to convert measurements data to json")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	_, err = conn.WriteMessages(
		kafka.Message{Value: json_data},
	)

	if err != nil {
		log.Println("Failed to write messages!")
		return nil, err
	}

	log.Println("[INFO]: Measurements Written to topic - ", DBName)
	return &pb.Reply{}, nil
}