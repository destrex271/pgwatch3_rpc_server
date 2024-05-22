package main

import (
    "net/http"
    "main/rpc"
)

func main() {
server := &internal.Server{} // implements Haberdasher interface
  twirpHandler := rpc.NewMetricsServer(server)
  http.ListenAndServe(":8080", twirpHandler)
}

