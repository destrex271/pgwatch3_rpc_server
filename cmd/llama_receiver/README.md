# LLM processing Unit

This sink example demonstrates how along with stroing measurements, we can also use similar setups to generate insights on the go from 3rd party tools. To demonstrate this capability we are using `tinyllama` which is compact 1.1B Llama model trained on 3 trillion tokens. 

## Features

- **Insight Generation**: We are using the `tinyllama` model to generate insights from most recent batches of measurements sent by pgwatch. You can decide the batch size accoding to your requirements, the default value is set to 10.

- **Applications**: These insights can be used to catch problems with the database instance in advance. Although we do not recommend completely relying on these since llms do have a tendency to halucinate. Nonetheless this is one of the places where llms can be utilized.

## Dependencies

 - **Ollama**: We are using Ollama to interact with the `tinyllama` model. You can use the docker image from here
 
 - **Postgres**: We are using postgres to store the measurements and the insights generated. To see the insights generated you can run a `select * from insights` on your database. 

## Usage

```bash

./cmd/llama_receiver/setup.sh # To start ollama docker image

go run ./cmd/llama_receiver --port=<port_number_for_sink> --serverURI=<ollama_server_uri> --pgURI=<pgURI> --batchSize=10
```