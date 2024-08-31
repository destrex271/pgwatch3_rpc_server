package sinks

import (
	"encoding/json"
	"log"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
)

func GetJson[K map[string]string | map[string]any | float64 | api.MeasurementEnvelope | api.Metric](value K) string {
	jsonString, err := json.Marshal(value)
	if err != nil {
		log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
	}
	return string(jsonString)
}
