package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ESReceiver struct {
	esClient *elasticsearch.Client
	sinks.SyncMetricHandler
}

func NewESReceiver(addrs []string, username, password, cacertPath string) (*ESReceiver, error) {
	cacert, err := os.ReadFile(cacertPath)
	if err != nil {
		return nil, err
	}

	esCfg := elasticsearch.Config{
		Addresses: addrs,
		Username: username,
		Password: password,
		CACert: cacert,
	}

	esClient, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, err
	}

	es := &ESReceiver{
		esClient: esClient,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}
	go es.HandleSyncMetric()
	return es, nil
}

func (es *ESReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	var err error
	for _, dataItem := range msg.GetData() {
		jsonData, err2 := json.Marshal(dataItem)
		if err2 != nil {
			err = errors.Join(err, err2)
			continue
		}

		req := esapi.IndexRequest{
			Index: msg.GetDBName() + "_" + msg.GetMetricName(), 
			Body: bytes.NewReader(jsonData),
		}

		res, err2 := req.Do(ctx, es.esClient)
		if res != nil {
			log.Println(res)
		}

		if err2 != nil {
			err = errors.Join(err, err2)
		}
	}

	if err != nil {
		return nil, err
	}
	return &pb.Reply{Logmsg: "Measurement inserted."}, nil
}