# Kafka Producer Receiver

A Kafka Producer Receiver for handling metrics from PostgreSQL databases. This implementation creates a separate topic for each Database
and pushed the measurements as JSON in these Topics.

## Features

- **Dynamic Topic Management**: Automatically add or remove PostgreSQL databases as topics based on incoming metrics.
- **Error Handling**: Robust error handling for connection management and message writing.
- **JSON Serialization**: Serialize measurement data to JSON before sending it to Kafka.

## Usage
```bash
go run ./cmd/kafka_prod_receiver --port=<port_number_for_sink> --kafkaHost=<host_address_of_kafka> --autoadd=<true/false>
```

## Command-Line Flags
 - *port*: Specify the port where the sink will receive measurements (required).
 - *kafkaHost*: Specify the host and port of the Kafka instance (default is localhost:9092).
 - *autoadd*: Enable or disable automatic addition of new databases as Kafka topics (default is true).
 