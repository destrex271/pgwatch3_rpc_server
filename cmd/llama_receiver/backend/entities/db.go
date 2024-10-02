package entities

import (
	"context"
	"log"
	"main/config"
)

type DB struct {
	Id               int    `json:"id"`
	Name             string `json:"name"`
	MeasurementCount int    `json:"int"`
}

func GetDBs(ctx context.Context) ([]DB, error) {
	rows, err := config.Conn.Query(ctx, "SELECT db.id, db.dbname, COUNT(measurement.data) FROM db LEFT JOIN measurement ON db.id = measurement.database_id GROUP BY db.id;")

	log.Println(rows)

	if err != nil {
		return nil, err
	}

	var db_data []DB

	for rows.Next() {
		var db DB
		err = rows.Scan(&db.Id, &db.Name, &db.MeasurementCount)
		if err != nil {
			return nil, err
		}

		db_data = append(db_data, db)
	}

	return db_data, nil
}
