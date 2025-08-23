package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

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

	// Heath Check Ping call
	_, err = esClient.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Elasticsearch: %w", err)
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

		indexName := strings.ToLower(msg.GetDBName() + "_" + msg.GetMetricName())
		req := esapi.IndexRequest{
			Index: indexName, 
			Body: bytes.NewReader(jsonData),
		}

		res, err2 := req.Do(ctx, es.esClient)
		if err2 != nil {
			err = errors.Join(err, err2)
			continue
		}

		if res != nil {
			if res.IsError() {
				var errorBody map[string]any
				if err2 = json.NewDecoder(res.Body).Decode(&errorBody); err2 == nil {
					err = errors.Join(err, fmt.Errorf("elasticsearch error [%s]: %v", res.Status(), errorBody))
				} else {
					err = errors.Join(err, fmt.Errorf("elasticsearch error [%s]", res.Status()))
				}
			}
			_ = res.Body.Close()
		}
	}

	if err != nil {
		return nil, err
	}
	return &pb.Reply{Logmsg: "Measurement Indexed."}, nil
}