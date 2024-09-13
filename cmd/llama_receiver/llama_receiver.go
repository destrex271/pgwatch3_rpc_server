package main

import (
	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/rifaideen/talkative"
)

const contextString = "You are an expert model in extracting critical information out of PostgreSQL database metrics and measurements. I'll be providing you with a set of measurements and I need you to derive insights using the previous measurements. Your output should be in JSON format."

type LlamaReceiver struct {
	Client    *talkative.Client
	Context   string
	ServerURI string
	sinks.SyncMetricHandler
}

func NewLlamaReceiver(llmServerURI string) (recv *LlamaReceiver, err error) {
	client, err := talkative.New(llmServerURI)

	if err != nil {
		return nil, err
	}

	recv = &LlamaReceiver{
		Client:            client,
		Context:           contextString,
		ServerURI:         llmServerURI,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return recv, nil
}

func (r *LlamaReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	return nil
}
