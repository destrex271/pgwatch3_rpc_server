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

func (handler *SyncMetricHandler) SyncMetricHandler(syncReq *SyncReq, logMsg *string) error {
	if len(syncReq.OPR) == 0 {
		return errors.New("Empty Operation.")
	}
	if len(syncReq.DBName) == 0 {
		return errors.New("Empty Database.")
	}
	if len(syncReq.MetricName) == 0 {
		return errors.New("Empty Metric Provided.")
	}

	go handler.PopulateChannel(syncReq)
	return nil
}

func (handler *SyncMetricHandler) PopulateChannel(syncReq *SyncReq) {
	handler.SyncChannel <- *syncReq
}

func (handler *SyncMetricHandler) GetSyncChannelContent() SyncReq {
	content := <-handler.SyncChannel
	return content
}
