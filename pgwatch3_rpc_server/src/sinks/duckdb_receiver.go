package sinks

/**
*
* Similar to PostgresWriter in the main postgres repository: https://github.com/cybertec-postgresql/pgwatch3/blob/master/src/sinks/postgres.go
*
 */

import (
	"context"

	"github.com/marcboeker/go-duckdb"
)


type DuckDBReceiver struct{
}

type DuckDBWriter struct{
   conn   duckdb.Connector
}

func NewDuckDBWriter(dbname string) (*DuckDBWriter, error){
    conn, err := duckdb.NewConnector(dbname + ".db", nil)
    wr := new(DuckDBWriter)

    if err != nil{
        return wr, err
    }

    wr.conn = *conn
    return wr, nil
}

func (r *DuckDBReceiver) UpdateMeasurements(msg *MeasurementMessage, logMsg *string) error{ 

    dbWriter, err := NewDuckDBWriter(msg.DBName)

    if err != nil{
        *logMsg = err.Error()
        return err
    }

    // Get Connection
    dbConn, err := dbWriter.conn.Connect(context.Background())

    if err != nil{
        *logMsg = err.Error()
        return err
    }
    defer dbConn.Close()

    return nil
}
