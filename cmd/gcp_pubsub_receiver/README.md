# GCP Pub/Sub Receiver

A gRPC server that writes metrics received from pgwatch
to Google cloud pub/sub servers.

- The receiver creates a new topic called `pgwatch` in the provided GCP project.
- The receiver uses the official pub/sub package for golang which supports Authentication via [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials)

## Usage example

```bash
go run ./cmd/gcp_pubsub_receiver --port <grpc-server-port-number> --project-id <gcp-project-id>
```
