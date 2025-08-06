[![Tests](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml/badge.svg)](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/destrex271/pgwatch3_rpc_server/badge.svg?branch=main)](https://coveralls.io/github/destrex271/pgwatch3_rpc_server?branch=main)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/destrex271/pgwatch3_rpc_server)
![GitHub Repo stars](https://img.shields.io/github/stars/destrex271/pgwatch3_rpc_server)


# Pgwatch3 RPC Receivers
This repository contains the essential components to build your own Remote Sinks for Pgwatch v3. You can find the basic structure to create a Sink (or Receiver as we call it in this repo) which is basically a RPC Server that the pgwatch RPC Client interacts with.

The primary goal of this repository is to provide you with the building blocks to get started with your own implementations and also to provide some examples of places where measurements from pgwatch can be used. You can find some of our example implementations in the cmd folder.

Checkout <a href="https://github.com/cybertec-postgresql/pgwatch">PgWatch</a> to get started with this project.

## Architecture
The Remote Sinks work using RPC protocol. Some cool advantages of using RPC are:
 - PgWatch is not concerned about the actual sink implementation. You can literally do anything with the measurements delivered to you by pgwatch and share messages per function call if required.
 - The sink implementations can be easily developed in Go, which has support for most of the storage formats out there and is pretty easy to write and work with. 


![image](https://github.com/user-attachments/assets/a759597f-6369-4716-bbd0-573281c54445)

![image](https://github.com/user-attachments/assets/8a09a6fe-5fd2-4c55-b0a2-47a92ed12c3a)


The RPC receiver is treated as the default sink formats and no special changes are required in your pgwatch setup. 

To use a RPC sink you can start pgwatch with the argument: `--sink=rpc://<host>:<port>`.

## Running Custom RPC Sinks
By default the RPC Server for your sink listens at 0.0.0.0 with the specified port number.
To start any of the given receivers you can use:

```bash
# Parquet Receiver
go run ./cmd/parquet_receiver --port=<port_number_for_sink> --rootFolder=<location_on_disk>

# Kafka Receiver
go run ./cmd/kafka_prod_receiver --port=<port_number_for_sink> --kafkaHost=<host_address_of_kafka> --autoadd=<true/false>
```

Now once your receiver is up you can setup pgwatch as follows:
```bash
go run ./cmd/pgwatch --sources=<postgres://postgres@localhost:5432/postgres> --sink=rpc://<ip/hostname_of_your_sink>:<port_where_recv_is_listening>
```

Voila! You have a seamless integration between pgwatch and your custom sink. Try out our various implementations to get a feel of how these receivers feel with your custom pgwatch instances.

## Developing Custom Sinks

To develop your own custom sinks, refer to this mini [tutorial](TUTORIAL.md).

You can also look at our example sinks to help with your implementation or extend them for your own use cases:

- [CSV Receiver](/cmd/csv_receiver/README.md): Store measurements in CSV files.
- [Kafka Receiver](/cmd/kafka_prod_receiver/README.md): Stream measurements using Kafka.
- [Parquet Receiver](/cmd/parquet_receiver/README.md): Store measurements in Parquet files.
- [ClickHouse Receiver](/cmd/clickhouse_receiver/README.md): Store measurements in OLAP databases like ClickHouse for analytics.
- [LLama Receiver](/cmd/llama_receiver/README.md): Gain performance insights and recommendations from your measurements using `tinyllama`.
- [S3 Receiver](/cmd/s3_receiver/README.md): Store measurements in AWS S3.