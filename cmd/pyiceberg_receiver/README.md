# Iceberg Receiver

A gRPC server that writes metrics received from pgwatch in Iceberg Table format.

- The server assumes a PostgreSQL catalog is used and creates `pgwatch` namespace and `pgwatch.metrics` table within it if they don't exist.
- The table is partitioned by `MetricName` and `DBName` (in order).
- Metrics are written in the local file system as Apache Arrow records with the following schema:
    ```python
    Schema(
        NestedField(field_id=1, name="DBName", field_type=StringType(), required=True),
        NestedField(field_id=2, name="MetricName", field_type=StringType(), required=True),
        NestedField(field_id=3, name="Data", field_type=StringType(), required=True),
    )
    ```
- Catalog configurations should be provided in [.pyiceberg.yaml](./.pyiceberg.yaml) file under `pgcatalog` see [PyIceberg SQL Catalog](https://py.iceberg.apache.org/configuration/#sql-catalog) for details.

## Flags

```bash
usage: pyiceberg_receiver [-h] -p PORT -d DIR

options:
  -h, --help            show this help message and exit
  -p PORT, --port PORT  The port number to use for the gRPC server.
  -d DIR, --iceberg-data-dir DIR
                        Directory to store iceberg tables in.
```

## Usage example

```bash
# generate python gRPC code from protobuf
python3 -m grpc_tools.protoc -I sinks/pb --python_out=cmd/pyiceberg_receiver --grpc_python_out=cmd/pyiceberg_receiver sinks/pb/pgwatch.proto
# install dependencies
pip install -r requirements.txt
# tell PyIceberg about the dir to look for .pyiceberg.yaml in
export PYICEBERG_HOME="cmd/pyiceberg_receiver"
# run the server
python3 cmd/pyiceberg_receiver -p <grpc-server-port> -d <dir-path>
```

## TODO

- [ ] Use object storage instead of the local file system.
- [ ] Support TLS over the gRPC connection.
- [ ] Add authentication interceptor.
- [ ] Cache measurements to minimize the number of Parquet files written.