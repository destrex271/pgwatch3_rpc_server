package sinks

import (
	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

type SyncReq struct {
	OPR        string
	DBName     string
	MetricName string
}

type Receiver interface {
	UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error
}

type SyncMetricHandler struct {
	SyncChannel chan SyncReq
}
