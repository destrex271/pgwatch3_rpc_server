package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (receiver *Receiver) UpdateMeasurements(writeRequest *WriteRequest, status *int) error {
    log.Print("Received metrics: " + writeRequest.Msg.DBName)
	if receiver.sink_type == CSV {
		writer := new(CSVReceiver)
        err := writer.UpdateMeasurements(writeRequest, status)
        if err != nil{
            return err
        }
	}else if receiver.sink_type == TEXT{
        writer := new(TextReceiver)
        err := writer.UpdateMeasurements(writeRequest, status)
        if err != nil{
            return err
        }
    }else{
        return errors.New("No writer was specified")
    }
	return nil
}

func main() {

	receiverType := flag.String("type", "", "The type of sink that you want to keep this node as.\nAvailable options:\n\t- csv\n\t- text")
    port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	flag.Parse()

    if *port == "-1"{
        log.Fatal("[ERROR]: No Port Specified")
        return
    }

    log.Println("Setting up Server.....")
	server := new(Receiver)

	if *receiverType == "csv" {
		server.sink_type = CSV
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
