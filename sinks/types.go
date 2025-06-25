package sinks

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type Receiver interface {
	UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error
	SyncMetric(syncReq *api.RPCSyncRequest, logMsg *string) error
}

type RPCCreds struct {
	Username string
	Password string
}

type EnvelopeWrapper struct {
	Msg *api.MeasurementEnvelope
	RPCCreds
}

type AuthWrapper struct {
	Sink Receiver
	RPCCreds
}

type SyncReqWrapper struct {
	SyncReq *api.RPCSyncRequest
	RPCCreds
}

func NewAuthWrapper(sink Receiver) *AuthWrapper {
	return &AuthWrapper{
		Sink: sink,
		RPCCreds: RPCCreds{
			Username: os.Getenv("RPC_USERNAME"),
			Password: os.Getenv("RPC_PASSWORD"),
		},
	}
}

func (w *AuthWrapper) IsAuthenticated(username, password string) bool {
	return (w.Password == "" || w.Password == password) && (w.Username == "" || w.Username == username)
}

func (w *AuthWrapper) UpdateMeasurements(req *EnvelopeWrapper, logMsg *string) error {
	if !w.IsAuthenticated(req.Username, req.Password) {
		return fmt.Errorf("unauthorized")
	}

	return w.Sink.UpdateMeasurements(req.Msg, logMsg)
}

func (w *AuthWrapper) SyncMetric(req *SyncReqWrapper, logMsg *string) error {
	if !w.IsAuthenticated(req.Username, req.Password) {
		return fmt.Errorf("unauthorized")
	}

	return w.Sink.SyncMetric(req.SyncReq, logMsg)
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
		handler.GetSyncChannelContent()
		// do nothing we don't care
	}
}