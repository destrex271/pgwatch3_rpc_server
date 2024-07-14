package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (receiver *Receiver) UpdateMeasurements(msg *MeasurementMessage, status *int) error {
    log.Print("Received metrics: " + msg.DBName)
    if len(msg.DBName) == 0{
        log.Print("Empty")
        return nil
    }
	if receiver.sink_type == CSV {
		writer := new(CSVReceiver)
        err := writer.UpdateMeasurements(msg, status, receiver.storage_folder, receiver)
        if err != nil{
            return err
        }
	}else if receiver.sink_type == TEXT{
        writer := new(TextReceiver)
        err := writer.UpdateMeasurements(msg, status)
        if err != nil{
            return err
        }
    }else{
        return errors.New("No writer was specified")
    }
	return nil
}

// Gets the sync request from pgwatch3 and adds it to the receiver.SyncChannel
func (receiver *Receiver) SyncMetricSignal(syncReq *SyncReq, logMsg *string) error {
    log.Print("RECEVIED SIGNAL")
    go receiver.PopulateChannel(syncReq)
    log.Print("Logged signal")
	return nil
}

func (receiver *Receiver) PopulateChannel(syncReq *SyncReq){
    receiver.SyncChannel <- *syncReq
}

func (receiver *Receiver) GetSyncChannelContent() SyncReq{
    content := <- receiver.SyncChannel
    return content
}

func main() {

    // Important Flags
	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
    port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
    storage_folder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

    if *port == "-1"{
        log.Fatal("[ERROR]: No Port Specified")
        return
    }

    log.Println("Setting up Server.....")
	server := new(Receiver)

    server.SyncChannel = make(chan SyncReq, 10)
	if *receiverType == "csv" {
		server.sink_type = CSV
        server.storage_folder = *storage_folder
	} else if *receiverType == "text" {
		// Only for testing
		server.sink_type = TEXT
	} else {
		// Throw Error
		server.sink_type = NONE
        log.Fatal("[ERROR]: No Sink Type was provided. Please use the --type option")
		return
	}

	rpc.Register(server)
    log.Println("RPC registered")
	rpc.HandleHTTP()

    log.Println("listening...")
    listener, err := net.Listen("tcp", "0.0.0.0:" + *port)

    log.Println("Found -> ", listener)
	if err != nil {
		log.Fatal(err)
	}

	http.Serve(listener, nil)
}
