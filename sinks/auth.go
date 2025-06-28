package sinks

import (
	"fmt"
	"os"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type RPCServerCreds struct {
	Username string
	Password string
}

type EnvelopeWrapper struct {
	Msg *api.MeasurementEnvelope
	RPCServerCreds
}

type AuthWrapper struct {
	Sink Receiver
	RPCServerCreds
}

type SyncReqWrapper struct {
	SyncReq *api.RPCSyncRequest
	RPCServerCreds
}

func NewAuthWrapper(sink Receiver) *AuthWrapper {
	return &AuthWrapper{
		Sink: sink,
		RPCServerCreds: RPCServerCreds{
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

