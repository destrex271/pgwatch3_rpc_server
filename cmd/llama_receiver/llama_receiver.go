package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/jackc/pgx/v5"
	"github.com/rifaideen/talkative"
)

const contextString = "You are an expert in extracting critical information out of PostgreSQL database metrics and measurements. I'll be providing you with a set of measurements for a single metric of a database. I need you to derive insights from them. Do all this analysis and provide me with a report about your insights and suggestions from studying the measurements provided.\nThe metric name and measurements are:\n{DATA}.\nProvide me with your analysis.\nI don't want methods, just give me your observations. Your output format should be [Metric : Your Analysis]"

type LlamaReceiver struct {
	Client    *talkative.Client
	Context   string
	Ctx       context.Context
	ServerURI string
	DbConn    *pgx.Conn
	sinks.SyncMetricHandler
}

type MeasurementsData struct {
	DbID       uint
	MetricName string
	data       string
}

func NewLlamaReceiver(llmServerURI string, pgURI string, ctx context.Context) (recv *LlamaReceiver, err error) {
	client, err := talkative.New(llmServerURI)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(ctx, pgURI)
	if err != nil {
		return nil, err
	}

	recv = &LlamaReceiver{
		Client:            client,
		Context:           contextString,
		Ctx:               ctx,
		ServerURI:         llmServerURI,
		DbConn:            conn,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	return recv, nil
}

func (r *LlamaReceiver) SetupTables() error {
	_, err := r.DbConn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS Db(id serial Primary key, dbname varchar(255))`)
	if err != nil {
		return err
	}

	_, err = r.DbConn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS Measurement (
		created_time TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		data JSONB,
		metric_name VARCHAR(255),
		database_id SERIAL,
		FOREIGN KEY (database_id) REFERENCES Db(id)
	);`)
	if err != nil {
		return err
	}

	_, err = r.DbConn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS Insights(
		insight_data text, 
		database_id serial, 
		created_time TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		foreign key (database_id) references Db(id) 
	)`)
	if err != nil {
		return err
	}

	return nil
}

func (r *LlamaReceiver) AddMeasurements(msg *api.MeasurementEnvelope) error {
	var id int
	// Try to fecth id
	err := r.DbConn.QueryRow(r.Ctx, `SELECT id FROM Db WHERE dbname='`+msg.DBName+`'`).Scan(&id)
	if err != nil {
		if err.Error() != "no rows in result set" {
			return err
		}
		// if not found, add database to table
		_, err := r.DbConn.Exec(r.Ctx, `INSERT INTO Db(dbname) VALUES('`+msg.DBName+`')`)
		if err != nil {
			return err
		}

		// Get new id
		err = r.DbConn.QueryRow(r.Ctx, `SELECT id FROM Db WHERE dbname='`+msg.DBName+`'`).Scan(&id)
		if err != nil {
			return err
		}
	}

	// Convert measurement to json
	jsonData, err := json.Marshal(msg.Data)
	if err != nil {
		return err
	}

	// insert measurements with current timestamp(default) into table Measurement
	_, err = r.DbConn.Exec(r.Ctx, fmt.Sprintf(`INSERT INTO Measurement(data, database_id, metric_name) VALUES('%s', %d, '%s')`, string(jsonData), id, msg.MetricName))
	if err != nil {
		return err
	}

	return nil
}

func (r *LlamaReceiver) GetAllMeasurements(dbname string, metric_name string, context_size uint) ([]MeasurementsData, error) {
	query := fmt.Sprintf("Select database_id, metric_name, data from Measurement inner join Db on Measurement.database_id = Db.id where Db.dbname = '%s' ORDER BY created_time DESC LIMIT %d", dbname, context_size)
	log.Println(query)
	rows, err := r.DbConn.Query(r.Ctx, query)
	if err != nil {
		return nil, err
	}

	var data []MeasurementsData

	for rows.Next() {
		var cur_data MeasurementsData
		err = rows.Scan(&cur_data.DbID, &cur_data.MetricName, &cur_data.data)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println(cur_data)
		data = append(data, cur_data)
	}

	return data, nil
}

func (r *LlamaReceiver) PreparePrompt(dbname string, metric_name string) (string, error) {
	all_measurements, err := r.GetAllMeasurements(dbname, metric_name, 10)
	if err != nil {
		return "", err
	}

	var data_string string
	for _, measurement := range all_measurements {
		data_string += "Metric Name -> " + measurement.MetricName + ": " + measurement.data + ".\n"
	}

	final_msg := strings.ReplaceAll(r.Context, "{DATA}", data_string)
	return final_msg, nil
}

func (r *LlamaReceiver) GetDbID(dbname string) int {
	// Get id of database with name = dbname
	id := 0
	query := fmt.Sprintf(`SELECT id FROM Db where dbname='%s'`, dbname)
	err := r.DbConn.QueryRow(r.Ctx, query).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func (r *LlamaReceiver) AddInsights(dbid int, insights string) error {
	// Insert model response in table insights
	query := fmt.Sprintf(`INSERT INTO Insights(database_id, insight_data) VALUES(%d, '%s')`, dbid, insights)
	log.Println(query)
	_, err := r.DbConn.Exec(r.Ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (r *LlamaReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {

	// Check db name
	if len(msg.DBName) == 0 {
		return errors.New("empty database name")
	}

	// Check Metric name
	if len(msg.MetricName) == 0 {
		return errors.New("empty metric name")
	}

	// Check data length
	if len(msg.Data) == 0 {
		return errors.New("empty measurement list")
	}

	err := r.SetupTables()
	if err != nil {
		return err
	}

	err = r.AddMeasurements(msg)
	if err != nil {
		return err
	}

	final_msg, err := r.PreparePrompt(msg.DBName, msg.MetricName)
	if err != nil {
		return err
	}

	log.Println(final_msg)

	model := "tinyllama"
	model_response := ""
	// Callback function to handle the response
	callback := func(cr string, err error) {
		if err != nil {
			fmt.Println(err)

			return
		}

		var response talkative.ChatResponse

		if err := json.Unmarshal([]byte(cr), &response); err != nil {
			fmt.Println(err)

			return
		}

		model_response += response.Message.Content
	}

	var params *talkative.ChatParams = nil

	log.Println(final_msg)

	// The chat message to send
	message := talkative.ChatMessage{
		Role:    talkative.USER, // Initiate the chat as a user
		Content: final_msg,
	}

	done, err := r.Client.PlainChat(model, callback, params, message)

	if err != nil {
		panic(err)
	}

	<-done // wait for the chat to complete
	log.Println(model_response)
	id := r.GetDbID(msg.DBName)
	if id == -1 {
		return errors.New("unable to find database in records")
	}

	err = r.AddInsights(id, model_response)
	if err != nil {
		return err
	}

	return nil
}
