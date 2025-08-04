package main

import (
	"context"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

// struct type that implements pgwatch's required gRPC methods:
// 1. UpdateMeasurements() => Write new measurements to the chosen storage backend
// 2. SyncMetric() => close/open server resources for metrics/sources removed from or added to pgwatch monitoring
// 3. DefineMetrics() => write pgwatch metric definitions to the chosen storage backend (optional)
//
// if any of the above methods (e.g. DefineMetrics()) isn't implemented by the `Receiver` struct, gRPC's default
// Unimplemented method will be called
type Receiver struct {
	// Add Receiver Custom fields here.
	// e.g. database conn objects
	
	// We recommend embeding `sinks.SyncMetricHandler` struct
	//
	// It provides a default implementation for 
	// `SyncMetric()` that writes sync requests to a channel that
	// can be read from using `item, ok := receiver.GetSyncChannelContent()` 
	//
	// Then users should provide an implementation for a method that continuously
	// reads from the channel and opens/closes resources.
	//
	// Or use `SyncMetricHandler.HandleSyncMetric()` that ignores the request but only
	// reads from the channel to empty it.
	//
	// Otherwise, users should implement `SyncMetric()` directly on `Receiver` struct
	// and directly embed gRPC's `pb.UnimplementedReceiverServer`. 
	sinks.SyncMetricHandler
}

// create a new `Receiver` object
func NewReceiver(args ...any) (tr *Receiver) {
	// initialize `Receiver` required fields here.
	// e.g. connect to the target database

	syncReqsChanLen := 1024
	recv := &Receiver{
		// instantiate `SyncMetricHandler()`
		SyncMetricHandler: sinks.NewSyncMetricHandler(syncReqsChanLen),
	}

	// Invoke the `SyncMetric()` handler
	go recv.ReceiverSyncMetricHandler()
	// Or go recv.HandleSyncMetric()

	return recv
}


// Optional Method for handling `SyncMetric()` requests 
func (r Receiver) ReceiverSyncMetricHandler() {
	for {
		// read from channel
		req, ok := r.GetSyncChannelContent()
		if !ok {
			// channel has been closed
			return
		}

		switch req.Operation {
		case pb.SyncOp_AddOp:
			// open resources for `req.GetDBName()` and `req.GetMetricName()`.
		case pb.SyncOp_DeleteOp:
			// close resources for `req.GetDBName()-req.GetMetricName()`.
			// 
			// Note that when `req.GetMetricName()` is "" then pgwatch has removed 
			// the entire database, you should close resources for all its metrics.
		}
	}
}

// Write received Measurements to the desired storage backend
// 
// Measurements msg parameter has the following definition:
// type MeasurementEnvelope struct {
//   ...
//   DBName string => Source name
//   MetricName string => Metric name
//   CustomTags map[string]string => Custom tags associated with source (if any)
//   Data []*structpb.Struct => Metric query results
//   ...
// }
// accessible via msg.Get[fieldName]()
//
// errors returned will appear in pgwatch's logs as `[ERROR]` messages
// while `pb.Reply{Logmsg: "msg"}` returned will appear as `[INFO]` messages.
func (r Receiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	return nil, nil
}