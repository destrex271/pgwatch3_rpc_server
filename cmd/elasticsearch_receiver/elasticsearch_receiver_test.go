package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

var esContainer *elasticsearch.ElasticsearchContainer
var esHttpClient *http.Client

func TestMain(m *testing.M) {
	var err error
	esContainer, err = elasticsearch.Run(
		context.Background(),
		"docker.elastic.co/elasticsearch/elasticsearch:8.19.2",
		elasticsearch.WithPassword("pgwatch"),
		testcontainers.WithExposedPorts("9200:9200/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog(`"message":"started`),
		),
	)

	if err != nil {
		panic(err)
	}

	esHttpClient = http.DefaultClient
	// configure TLS transport based on the certificate bytes that were retrieved from the container
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(esContainer.Settings.CACert)
	esHttpClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	}

	exitCode := m.Run()
	if err := testcontainers.TerminateContainer(esContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
	os.Exit(exitCode)
}

func TestNewESReceiver(t *testing.T) {
	a := assert.New(t)

	ESCAPath := "es_http_ca.crt"
	err := os.WriteFile(ESCAPath, esContainer.Settings.CACert, 0644)
	if err != nil {
		panic(err)
	}
	defer func() {_ = os.Remove(ESCAPath)}()

	ESReceiver, err := NewESReceiver(
		[]string{esContainer.Settings.Address},
		"elastic",
		esContainer.Settings.Password,
		ESCAPath,
	)	
	a.NoError(err)
	a.NotNil(ESReceiver)

	t.Run("Test calling SyncMetric() from ES Receiver", func(t *testing.T) {
		req := testutils.GetTestRPCSyncRequest()
		reply, err := ESReceiver.SyncMetric(context.Background(), req)
		a.NoError(err)
		a.Equal(reply.GetLogmsg(), fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), "Add"))
	})

	t.Run("Test ES Receiver UpdateMeasurements()", func(t *testing.T) {
		msg := testutils.GetTestMeasurementEnvelope()
		reply, err := ESReceiver.UpdateMeasurements(context.Background(), msg)
		a.NoError(err)
		a.Equal(reply.GetLogmsg(), "Measurement Indexed.")

		// wait for the data to be indexed
		time.Sleep(2 * time.Second)

		req, err := http.NewRequest("GET", esContainer.Settings.Address + "/_count", nil)
		a.NoError(err)
		req.SetBasicAuth("elastic", esContainer.Settings.Password)
		resp, err := esHttpClient.Do(req)
		a.NoError(err)

		var data map[string]any
		json.NewDecoder(resp.Body).Decode(&data)
		a.Equal(float64(1), data["count"])
	})
}