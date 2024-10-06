[![Tests](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml/badge.svg)](https://github.com/destrex271/pgwatch3_rpc_server/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/destrex271/pgwatch3_rpc_server/badge.svg?branch=main)](https://coveralls.io/github/destrex271/pgwatch3_rpc_server?branch=main)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/destrex271/pgwatch3_rpc_server)
![GitHub Repo stars](https://img.shields.io/github/stars/destrex271/pgwatch3_rpc_server)


# Pgwatch3 RPC Receivers
This repository contains the essential components to build your own Remote Sinks for Pgwatch v3. You can find the basic structure to create a Sink(or Receiver as we call it in this repo) which is basically a RPC Server that the pgwatch RPC Client interacts with.

The primary goal of this repository is to provide you with the building blocks to get started with your own implementations and also to provide some examples of places where measurements from pgwatch can be used. You can find some of our example implementations in the cmd folder.

Checkout <a href="https://github.com/cybertec-postgresql/pgwatch">PgWatch</a> to get started with this project.

## Architecture
The Remote Sinks work using RPC protocol. Some cool advantages of using RPC are:
 - PgWatch is not concerned about the actual sink implementation. You can literally do anything with the measurements delievered to you by pgwatch and share messages per function call if requried.
 - The sink implementations can be easily developed in Go, which has support for most of the storage formats out there and is pretty easy to write and work with. 


![image](https://github.com/user-attachments/assets/a759597f-6369-4716-bbd0-573281c54445)

The RPC receiver is treated as the default sink formats and no special changes are required in your pgwatch setup. 

To use a RPC sink you can start pgwatch with the argument: `--sink=rpc://<host>:<port>`.

## Running Custom RPC Sinks
By default the RPC Server for your sink listens at 0.0.0.0 with the specified port number.
To start the any of the given receivers you can use:

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

Viola! You havea  seemless integration between pgwatch and your custom sink. Try out our various implementations to get a feel of how these receivers feel with your custom pgwatch instances.

## Developing custom sinks

To develop your own sinks you can utilize the template that we have used for each of the example receivers. You can copy the main.go file as it is and use the Receiver interface to write your own the UpdateMeasurements functions.
You'll need to import the package `github.com/destrex271/pgwatch3_rpc_server/sinks` to utilize the components to build your own sinks.

You can refer our exmaple sinks to develop your own sinks or extend these for your usecases:

 - [CSV Receiver](/cmd/csv_receiver/README.md): Store measurements in CSV files
 - [Kafka Receiver](/cmd/kafka_prod_receiver/README.md): Stream measurements using kafka
 - [Parquet Receiver](/cmd/parquet_receiver/README.md): Store measurements in parquet files
 - [Clickhouse Receiver](/cmd/clickhouse_receiver/README.md): Store measurements in OLAP databases like Clickhouse for analytics
 - [Llama Receiver](/cmd/llama_receiver/README.md): Gain performance insights and recommendations based on your measurements using the power of `tinyllama`
 - [S3 Receiver](/cmd/s3_receiver/README.md): Store measurements in AWS S3

To get a simple to use template with no additional configurations, you can use the [text receiver example](/cmd/text_receiver/main.go) provided along with the other receiver examples.
