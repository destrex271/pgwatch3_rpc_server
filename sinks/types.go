package sinks

import (
	"errors"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type Receiver interface {
	UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error
	SyncMetric(syncReq *api.RPCSyncRequest, logMsg *string) error
}

type SyncMetricHandler struct {
	SyncChannel chan api.RPCSyncRequest
}

func NewSyncMetricHandler(chanSize int) SyncMetricHandler {
	if chanSize == 0 {
		chanSize = 1024
	}
	return SyncMetricHandler{SyncChannel: make(chan api.RPCSyncRequest, chanSize)}
}

func (handler SyncMetricHandler) SyncMetric(syncReq *api.RPCSyncRequest, logMsg *string) error {
	if len(syncReq.Operation) == 0 {
		return errors.New("Empty Operation.")
	}
	if len(syncReq.DbName) == 0 {
		return errors.New("Empty Database.")
	}
	if len(syncReq.MetricName) == 0 {
		return errors.New("Empty Metric Provided.")
	}

	select {
	case handler.SyncChannel <- *syncReq:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("Timeout while trying to sync metric")
	}
}

func (handler SyncMetricHandler) GetSyncChannelContent() api.RPCSyncRequest {
	content := <-handler.SyncChannel
	return content
}
