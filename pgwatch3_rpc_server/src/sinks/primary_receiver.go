package sinks

import(
    "log"
    "errors"
)

func (receiver *Reciever) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error {
	log.Print("Received metrics: " + msg.DBName)
	if len(msg.DBName) == 0 {
		log.Print("Empty")
		return nil
	}
	if receiver.SinkType == CSV {
		writer := new(CSVReciever)
		err := writer.UpdateMeasurements(msg, logMsg, receiver.StorageFolder, receiver)
		if err != nil {
			return err
		}
	} else if{
        writer := new(ParquetReciever){

        }
    } else if receiver.SinkType == TEXT {
		writer := new(TextReciever)
		err := writer.UpdateMeasurements(msg, logMsg)
		if err != nil {
			return err
		}
	} else {
		return errors.New("No writer was specified")
	}
	return nil
}

// Gets the sync request from pgwatch3 and adds it to the receiver.SyncChannel
func (receiver *Reciever) SyncMetricSignal(syncReq *SyncReq, logMsg *string) error {
	log.Print("RECEVIED SIGNAL")
	go receiver.PopulateChannel(syncReq)
	log.Print("Logged signal")
	return nil
}

func (receiver *Reciever) PopulateChannel(syncReq *SyncReq) {
	receiver.SyncChannel <- *syncReq
}

func (receiver *Reciever) GetSyncChannelContent() SyncReq {
	content := <-receiver.SyncChannel
	return content
}
