package sinks

/**
*
* Takes a DuckDB Connection and writes to <DBNAME>.db file
*
 */

import (
	"github.com/marcboeker/go-duckdb"
)

type DuckDBReceiver struct {
}

type DuckDBWriter struct {
	conn duckdb.Connector
}

func NewDuckDBWriter(dbname string) (*DuckDBWriter, error) {
	conn, err := duckdb.NewConnector(dbname+".db", nil)
	wr := new(DuckDBWriter)

	if err != nil {
		return wr, err
	}

	wr.conn = *conn
	return wr, nil
}

func (r *DuckDBReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error {
	return nil
}
