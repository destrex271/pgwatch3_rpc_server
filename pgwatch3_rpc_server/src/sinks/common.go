package sinks

import(
    "log"
    "encoding/json"
)

func GetJson[K map[string]string | map[string]any | float64 | Measurement | Metric](value K) string{
    jsonString, err := json.Marshal(value)
    if err != nil{
        log.Default().Fatal("[ERROR]: Unable to parse Metric Definition")
    }
    return string(jsonString)
}
