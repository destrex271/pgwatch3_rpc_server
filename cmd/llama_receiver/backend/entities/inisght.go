package entities

import (
	"context"
	"log"
	"main/config"
	"time"
)

type Insights struct {
	Id          int       `json:"id"`
	Data        string    `json:"data"`
	DatabaseID  string    `json:"databaseID"`
	CreatedTime time.Time `json:"created_time"`
}

func GetAllInsightsForDB(ctx context.Context, id int) ([]Insights, error) {
	var insights []Insights
	log.Println(id)

	rows, err := config.Conn.Query(ctx, "SELECT insight_data, database_id, created_time FROM insights WHERE database_id=$1 ORDER BY created_time DESC;", id)
	log.Println(err)
	if err != nil {
		return nil, err
	}

	log.Println(rows)

	for rows.Next() {
		var insight Insights

		err = rows.Scan(
			&insight.Data,
			&insight.DatabaseID,
			&insight.CreatedTime,
		)

		insight.Id = len(insights) + 1

		if err != nil {
			return nil, err
		}

		insights = append(insights, insight)
	}

	return insights, nil
}
