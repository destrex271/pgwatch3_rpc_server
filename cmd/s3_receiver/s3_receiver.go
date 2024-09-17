package main

import (
	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

type S3Receiver struct {
	sinks.SyncMetricHandler
}

func (s3 *S3Receiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	return nil
}
