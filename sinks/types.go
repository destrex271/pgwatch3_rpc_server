package sinks

import (
	"errors"
	"time"
	"log"

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
	if syncReq.Operation != api.AddOp && syncReq.Operation != api.DeleteOp {
		return errors.New("invalid operation type")
	}
	if len(syncReq.DbName) == 0 {
		return errors.New("empty database")
	}
	if len(syncReq.MetricName) == 0 {
		return errors.New("empty metric provided")
	}

	select {
	case handler.SyncChannel <- *syncReq:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timeout while trying to sync metric")
	}
}

func (handler SyncMetricHandler) GetSyncChannelContent() (api.RPCSyncRequest, bool) {
	content, ok := <-handler.SyncChannel
	return content, ok
}

// default HandleSyncMetric() clears the channel
// in case of the SyncMetric() requests are not 
// handled by the listening receiver
func (handler SyncMetricHandler) HandleSyncMetric() {
	for {
		req, ok := handler.GetSyncChannelContent()
		if !ok {
			// channel is closed
			return
		}

		log.Println("[INFO]: handle Sync Request", req)
	}
}