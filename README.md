[![Tests](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/go.yml/badge.svg)](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/destrex271/pgwatch3_rpc_server/badge.svg?branch=main)](https://coveralls.io/github/destrex271/pgwatch3_rpc_server?branch=main)

# Pgwatch3 RPC Receivers

## Running custom Sinks

Open the project in two terminals.

In the first terminal start the server i.e. in the folder pgwatch3_rpc_server/src
Currently we are using a demo text receiver by default which will store the measurements in a text file.
Later on we'll add command line arguments to specify the sink type.

To start the any of the given receivers you can use:

```bash
# CSV Receiver
cd cmd/csv_receiver
go run . --rootFolder=<location_on_disk>

# Parquet Receiver
cd cmd/parquet_receiver
go run . --rootFolder=<location_on_disk>
```

## Developing custom sinks

To develop your own sinks you can utilize the template that we have used for each of the example receivers. You can copy the main.go file as it is and use the Receiver interface to write your own the UpdateMeasurements functions.
