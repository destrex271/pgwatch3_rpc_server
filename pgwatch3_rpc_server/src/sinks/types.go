package sinks

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
    CSV = 1
    TEXT = 2
    PARQUET = 3
    DUCKDB = 4
    NONE = -1
)

type SyncReq struct {
	OPR        string
	DBName     string
	PgwatchID  string
	MetricName string
}

type Receiver struct{
    SinkType int
    StorageFolder string // Only for CSV
    SyncChannel chan SyncReq // Channel to receive sync signals
}
