package sinks

import "log"


type ParqReciever struct{}

func (r *ParqReciever) UpdateMeasurements(msg *MeasurementMessage, logMsg *string, fullPath string, primary_receiver *Reciever) error {
    log.Default().Print("Parquet to be implemented yet!")
    return nil
}
