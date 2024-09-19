# Parquet Data Receiver

This project implements a Parquet data receiver for the pgwatch3 monitoring tool. It utilizes the `pgwatch/v3` library to receive measurement data streams and stores them in Parquet format.

## Functionalities

* Receives measurement data from pgwatch3 via RPC.
* Validates received data for database and metric name emptiness.
* Stores data points in Parquet files named after the database they belong to.
* Manages temporary buffer storage for Parquet files.

## Dependencies

* `github.com/destrex271/pgwatch3_rpc_server/sinks` (assumed to be a custom library)
* `github.com/cybertec-postgresql/pgwatch/v3/api`
* `github.com/parquet-go/parquet-go`

## Usage

This program acts as an RPC server and requires the following flags to operate:

* `-port`: (Required) Specify the port on which the server listens for incoming data streams.
* `-rootFolder`: (Optional) Define the base directory for storing Parquet files. Defaults to the current working directory.

**Example:**

```bash
./pgwatch3_rpc_server -port 8080 -rootFolder /data/metrics
```


This will start the server listening on port 8080 and store Parquet files within the `/data/metrics` directory.


### Data Structure

Parquet files are organized based on the database name. Each file stores data points in the following schema:

* `DBName`:  Name of the database the data belongs to (string)
* `SourceType`: Type of data source (string)
* `MetricName`: Name of the metric (string)
* `Data`: JSON encoded metric data (string)
* `Tags`: JSON encoded metric tags (string)
* `MetricDefinitions`: JSON encoded metric definitions (string)
* `SysIdentifier`: System identifier for the data source (string)
