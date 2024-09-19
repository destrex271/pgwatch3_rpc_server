[![Tests](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml/badge.svg)](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/destrex271/pgwatch3_rpc_server/badge.svg?branch=main)](https://coveralls.io/github/destrex271/pgwatch3_rpc_server?branch=main)

# Pgwatch3 RPC Receivers
This repository contains the essential components to build your own Remote Sinks for Pgwatch v3. You can find the basic structure to create a Sink(or Receiver as we call it in this repo) which is basically a RPC Server that the pgwatch RPC Client interacts with.

The primary goal of this repository is to provide you with the building blocks to get started with your own implementations and also to provide some examples of places where measurements from pgwatch can be used. You can find some of our example implementations in the cmd folder.

Checkout <a href="https://github.com/cybertec-postgresql/pgwatch">PgWatch</a> to get started with this project.

## Running custom Sinks

Open the project in two terminals.

In the first terminal start the server i.e. in the folder pgwatch3_rpc_server/src
Currently we are using a demo text receiver by default which will store the measurements in a text file.
Later on we'll add command line arguments to specify the sink type.

To start the any of the given receivers you can use:

```bash
# Parquet Receiver
go run ./cmd/parquet_receiver --port=<port_number_for_sink> --rootFolder=<location_on_disk>

# Kafka Receiver
go run ./cmd/kafka_prod_receiver --port=<port_number_for_sink> --kafkaHost=<host_address_of_kafka> --autoadd=<true/false>
```

## Developing custom sinks

To develop your own sinks you can utilize the template that we have used for each of the example receivers. You can copy the main.go file as it is and use the Receiver interface to write your own the UpdateMeasurements functions.
