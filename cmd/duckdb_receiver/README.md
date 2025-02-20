# DuckDB Data Receiver

This receiver stores the metrics received from pgwatch into [DuckDB](https://duckdb.org/).

## Functionalities

* Receives measurement data from pgwatch3 via RPC.
* Validates received data for database name, metric name, and empty measurements.
* Stores data in DuckDB in a configurable table with the following schema:

```SQL
CREATE TABLE IF NOT EXISTS measurements(
    dbname VARCHAR,
    metric_name VARCHAR,
    data JSON,
    custom_tags JSON,
    metric_def JSON,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dbname, timestamp)
)
```

## Dependencies

* `github.com/destrex271/pgwatch3_rpc_server/sinks`
* `github.com/cybertec-postgresql/pgwatch/v3/api`
* `github.com/marcboeker/go-duckdb`

## Usage

This program acts as an RPC server and requires the following flags to operate:

* `-port`: (Required) Specify the port on which the server listens for incoming data streams.
* `-dbPath`: (Optional) Path to the DuckDB database file. Defaults to "metrics.duckdb".
* `-tableName`: (Optional) Name of the table to store measurements. Defaults to "measurements".

**Example:**

```bash
go run ./cmd/duckdb_receiver --port=9876
```

This will start the server listening on port 9876 and store the metrics in a DuckDB database.
