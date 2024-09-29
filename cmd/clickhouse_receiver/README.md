# ClickhouseDB Data Receiver

This receiver stores the metrics received from pgwatch into ClickHouse DB.

*Requires JSON enabled*

## Functionalities

* Receives measurement data from pgwatch3 via RPC.
* Validates received data for database and metric name emptiness.
* Stores data in ClickHouse in `Measurements` table with the following schema:

```SQL
-- If JSON type not allowed
CREATE TABLE IF NOT EXISTS Measurements(dbname String,custom_tags Map(String, String),metric_def String,real_dbname String,system_identifier String,source_type String,data String,timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp))

-- To use JSON type please ensure that either you are on the latest version of clickhouse or your have allow_experimental_object_type=1
-- If enabled the receiver will automatically detect that and create the new table if required
-- If JSON type allowed
CREATE TABLE IF NOT EXISTS Measurements(dbname String,custom_tags Map(String, String),metric_def JSON,real_dbname String,system_identifier String,source_type String,data JSON,timestamp DateTime DEFAULT now(),PRIMARY KEY (dbname, timestamp))
```

## Dependencies

* `github.com/destrex271/pgwatch3_rpc_server/sinks` (assumed to be a custom library)
* `github.com/cybertec-postgresql/pgwatch/v3/api`
* `github.com/ClickHouse/clickhouse-go/v2`

## Usage

This program acts as an RPC server and requires the following flags to operate:

* `-port`: (Required) Specify the port on which the server listens for incoming data streams.

You need to have the following environment variables configured before you run the receiver:
```bash
export user=<username>
export password=<passwd>
export dbname=<dbname>
export serverURI=<clickhouse_server_uri: NATIVE PORT> # Please check that you are using the native port and not the http port as the receiver is configured to utilize the native port
```

**Example:**

```bash
./pgwatch3_rpc_server -port=8080
```


This will start the server listening on port 8080 and store the metrics in your clickhouse db instance