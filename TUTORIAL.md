# Receiver Dev Tutorial

This provides a minimal tutorial for developing custom receivers.

This repo consists of two main directories: `sinks/` and `cmd/`.
- The `sinks/` directory contains logic shared by all sinks:
    - gRPC Server registration and startup, interceptors (auth, message validation), and TLS support logic.
    - A shared `SyncMetric()` implementation.

- The `cmd/` directory contains sink-specific logic. 
	- Each sink has its own folder, which contains:
		- `main.go`: the entry point for the receiver.
		- `[receiver_name]_receiver.go`: the implementation of the receiver logic.

To develop a new receiver, create a new `cmd/[receiver-dir-name]` directory containing `main.go` and `[receiver_name]_receiver.go` files, 
and follow the implementation instructions below.

## Receiver Files

### main.go

This file defines the `main()` function and is responsible for:

1. Parsing necessary server and storage-backend flags or environment variables.
2. Instantiating the receiver object using `NewReceiver(...)`.
3. Invoking `ListenAndServe()` to start the server.

```go
import (
	...
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
    // Here users should parse any necessary server and 
    // storage-backend flags or env vars (e.g. port number, database uri) 
    port := flag.String("port", "9999", "Description.")
    some_important_arg := flag.String("arg", "default-value", "Description.")
    flag.Parse()
    sink_required_env := os.Getenv("MY_REQUIRED_ENV")

    // maybe do some checks on them
    if sink_required_env == "some value" {
        log.Fatal("invalid value for `MY_REQUIRED_ENV`")
    }

    // instantiate new receiver object with parsed args 
    server := NewReceiver(sink_required_env, some_important_arg)
    // invoke pre-defined `sinks.ListenAndServe()` to start the server 
    // passing receiver object to use and port number to listen on
    if err := sinks.ListenAndServe(server, *port); err != nil {
        log.Fatal(err)
    }
}
```

### receiver.go

This file provides the core sink-specific implementation of the [`pgwatch` gRPC API](https://github.com/cybertec-postgresql/pgwatch/blob/master/api/pb/pgwatch.proto) methods.

```go
import (
	...
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

// All methods have the `(*pb.Reply, error)` return type,
// if error is returned it will appear in pgwatch's logs as `[ERROR]` messages,
// if no error and `pb.Reply{Logmsg: "your-message"}` is returned 
// it will appear in logs as `[INFO]` messages.

// Writes received Measurements to the desired storage backend
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
func (r Receiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	// Your storage solution insert logic

	// This will return `[]*structpb.Struct` which is one of protobuf's 
	// equivalents for pgwatch's measurements data structure `[]map[string]any`
	// mapping column names to their returned values
	//
	// individual elements can be converted back to `map[string]any`
	// using `msg.GetData()[i].AsMap()` or you can 
	// directly serialize the whole data to json using `json.Marshal(msg.GetData())`
	msg.GetData()

	return nil, nil
}

// Optional Custom `SyncMetric()` implementation that overrides
// `sinks.SyncMetricHandler`'s default one
//
// SyncReq has the following definition:
// type SyncReq struct {
// 	...
// 	MetricName string => metric removed from or added to specific source in pgwatch
// 	DBName string => source name
// 	Operation SyncOp => either add `pb.SyncOp_AddOp` or delete `pb.SyncOp_DeleteOp` operations
// 	...
// }
// accessible via req.Get[fieldName]()
func (r Receiver) SyncMetric(ctx context.Context, req *pb.SyncReq) (*pb.Reply, error) {
	return nil, nil
}
```

## Usage

To start using your newly developed receiver:

```
go generate ./sinks/pb # generate golang code from protobuf 
go run ./cmd/[receiver-dir-name] [OPTIONS]
```