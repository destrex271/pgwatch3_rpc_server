package sinks

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

func GetJson[K map[string]string | map[string]any | float64 | api.MeasurementEnvelope | api.Metric](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}

func (handler *SyncMetricHandler) SyncMetricHandler(syncReq *api.RPCSyncRequest, logMsg *string) error {
	if len(syncReq.Operation) == 0 {
		return errors.New("Empty Operation.")
	}
	if len(syncReq.DbName) == 0 {
		return errors.New("Empty Database.")
	}
	if len(syncReq.MetricName) == 0 {
		return errors.New("Empty Metric Provided.")
	}

	go handler.PopulateChannel(syncReq)
	return nil
}

func (handler *SyncMetricHandler) PopulateChannel(syncReq *api.RPCSyncRequest) {
	handler.SyncChannel <- *syncReq
}

func (handler *SyncMetricHandler) GetSyncChannelContent() api.RPCSyncRequest {
	content := <-handler.SyncChannel
	return content
}
