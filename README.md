# Pgwatch3 RPC Receivers

## Running the dummy receiver and client

Open the project in two terminals.

In the first terminal start the server i.e. in the folder pgwatch3_rpc_server/src
Currently we are using a demo text receiver by default which will store the measurements in a text file.
Later on we'll add command line arguments to specify the sink type.

To start the receiver:

```bash
# For Text sinks
pgwatch3_rpc_server/src git:(main)$ go run . --type=text --port:5050
# For CSV sinks
pgwatch3_rpc_server/src git:(main)$ go run . --type=csv --port:5050
```


In the second terminal switch to the directory `dummy_client/src` and run:

```bash
# Sends Measurements 40 times continously to the receiver
for i in {1..40}; do go run .; done;
```


## Tasks

 - <del>Implementing a dummy receiver and client to send the MeasurementMessage Struct</del>
 - <del>Testing if the net/rpc package is suitable for our usecase</del>
