package sinks

import (
	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type Receiver interface {
	UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error
}

type SyncMetricHandler struct {
	SyncChannel chan api.RPCSyncRequest
}
