package sinks

import "errors"

type SQLs map[int]string

type Metric struct {
	SQLs            SQLs
	InitSQL         string   `yaml:"init_sql,omitempty"`
	NodeStatus      string   `yaml:"node_status,omitempty"`
	Gauges          []string `yaml:",omitempty"`
	IsInstanceLevel bool     `yaml:"is_instance_level,omitempty"`
	StorageName     string   `yaml:"storage_name,omitempty"`
	Description     string   `yaml:"description,omitempty"`
}

type Measurement map[string]any
type Measurements []map[string]any

type MeasurementMessage struct {
	DBName           string
	SourceType       string
	MetricName       string
	CustomTags       map[string]string
	Data             Measurements
	MetricDef        Metric
	RealDbname       string
	SystemIdentifier string
}

const (
	CSV     = 1
	TEXT    = 2
	PARQUET = 3
	NONE    = -1
)

type SyncReq struct {
	OPR        string
	DBName     string
	MetricName string
}

type Receiver interface {
	UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error
}

type SyncMetricHandler struct {
	SyncChannel chan SyncReq
}

func (handler *SyncMetricHandler) SyncMetricHandler(syncReq *SyncReq, logMsg *string) error {
	if len(syncReq.OPR) == 0 {
		return errors.New("Empty Operation.")
	}
	if len(syncReq.DBName) == 0 {
		return errors.New("Empty Database.")
	}
	if len(syncReq.MetricName) == 0 {
		return errors.New("Empty Metric Provided.")
	}

	go handler.PopulateChannel(syncReq)
	return nil
}

func (handler *SyncMetricHandler) PopulateChannel(syncReq *SyncReq) {
	handler.SyncChannel <- *syncReq
}

func (handler *SyncMetricHandler) GetSyncChannelContent() SyncReq {
	content := <-handler.SyncChannel
	return content
}
