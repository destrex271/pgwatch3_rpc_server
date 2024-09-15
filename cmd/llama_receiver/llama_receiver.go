package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/rifaideen/talkative"
)

const contextString = "You are an expert in extracting critical information out of PostgreSQL database metrics and measurements. I'll be providing you with a set of measurements for a single metric of a database. I need you to derive insights from them. Do all this analysis and provide me with a report about your insights and suggestions from studying the measurements provided.\nThe metric name is: {METRIC} and the measurements are: {DATA}. Provide your output in JSON format."

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

	// Convert msg to string
	string_measurements, err := json.Marshal(msg.Data)

	final_msg := strings.ReplaceAll(r.Context, "{METRIC}", msg.MetricName)
	final_msg = strings.ReplaceAll(final_msg, "{DATA}", string(string_measurements))

	if err != nil {
		return err
	}

	model := "tinyllama"
	// Callback function to handle the response
	callback := func(cr string, err error) {
		if err != nil {
			fmt.Println(err)

			return
		}

		var response talkative.ChatResponse

		if err := json.Unmarshal([]byte(cr), &response); err != nil {
			fmt.Println(err)

			return
		}

		fmt.Print(response.Message.Content)
	}

	// Additional parameters to include. (Optional)
	var params *talkative.ChatParams = nil

	log.Println(final_msg)

	// The chat message to send
	message := talkative.ChatMessage{
		Role:    talkative.USER, // Initiate the chat as a user
		Content: final_msg,
	}

	done, err := r.Client.PlainChat(model, callback, params, message)

	if err != nil {
		panic(err)
	}

	<-done // wait for the chat to complete
	log.Println()

	return nil
}
