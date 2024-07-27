package sinks

import (
	"log"
)

type ParqReceiver struct{}

type ParquetSchema struct{
    DBName string
    SourceType string
    MetricName string
    Data string // json string
    MetricDefinitions string // json string
    SysIdentifier string
}

func (r *ParqReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string, fullPath string, primary_receiver *Receiver) error {
    
}
