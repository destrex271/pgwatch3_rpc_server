# Elasticsearch Receiver
 
A gRPC server that receives metrics from pgwatch and writes them to Elasticsearch. 

- Metrics are indexed in indices named as `lowercase(<dbname>_<metricname>)`.

## Options

### Env vars
 `$ELASTIC_PASSWORD`: Password for HTTP Basic Authentication.

### Flags

```bash
-addrs string
    A comma separated list of Elasticsearch nodes to use. (default "https://localhost:9200")
-ca-file string
    Certificate Authority file path. (default "./http_ca.crt")
-port string
    Port number for the server to listen on.
-user string
    Username for HTTP Basic Authentication. (default "elastic")
```

## Usage example

```bash
export ELASTIC_PASSWORD="your_es_password"
go run ./cmd/elasticsearch_receiver -port 1234 -addrs=https://localhost:9200 -user=elastic -ca-file=http_ca.crt
```