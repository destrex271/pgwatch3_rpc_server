package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rifaideen/talkative"
)

const contextString = `
You are a PostgreSQL database metrics analyzer. Examine the following JSON data containing measurements for various Postgres database metrics:

{DATA}

Provide a concise analysis of these metrics, focusing on:
- Key performance indicators
- Anomalies or concerning trends
- Resource utilization patterns
- Potential bottlenecks or issues

Based on your analysis, offer:
- 2-3 actionable recommendations to improve database performance

Present your insights and recommendations in bullet points, prioritizing the most important findings.
Be concise in your output. Your ourput should not exceed 200 words.
Do not reprint the input measurements provided to you, just give the insights.
`

type Batch map[string][]*api.MeasurementEnvelope

type LlamaReceiver struct {
	Client    *talkative.Client
	Context   string
	Ctx       context.Context
	ServerURI string
	ConnPool  *pgxpool.Pool
	MsmtBatch Batch
	BatchSize int
	mu        sync.Mutex
	MsCount   int
	sinks.SyncMetricHandler
}

type MeasurementsData struct {
	DbID       uint
	MetricName string
	data       string
}

func NewLlamaReceiver(llmServerURI string, pgURI string, ctx context.Context, batchSize int) (recv *LlamaReceiver, err error) {
	client, err := talkative.New(llmServerURI)
	if err != nil {
		log.Println("[ERROR]: unable to initialize llm client")
		return nil, err
	}

	// To use in insight generation to avoid any stuff
	pgxpool_config, err := pgxpool.ParseConfig(pgURI)
	if err != nil {
		log.Println("[ERROR]: unable to create pgx pool config")
		return nil, err
	}

	pgxpool_config.MaxConns = 25
	pgxpool_config.MaxConnLifetime = 5 * time.Minute
	pgxpool_config.MaxConnIdleTime = 15 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, pgxpool_config)
	if err != nil {
		log.Println("[ERROR]: Unable to intialize conneciton pool.")
		return nil, err
	}

	recv = &LlamaReceiver{
		Client:            client,
		Context:           contextString,
		Ctx:               ctx,
		ServerURI:         llmServerURI,
		ConnPool:          pool,
		MsmtBatch:         make(Batch),
		BatchSize:         batchSize,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
		MsCount:           0,
	}

	err = recv.SetupTables()
	if err != nil {
		log.Println("[ERROR]: unable to setup tables: ", err)
		return nil, err
	}

	go recv.HandleSyncMetric()

	return recv, nil
}

func (r *LlamaReceiver) HandleSyncMetric() {
	req := <-r.SyncChannel

	// Acquire connetion
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		log.Println("[ERROR]: Unable to acquire connection")
		return
	}
	defer conn.Release()

	switch req.Operation {
	case "Add":
		conn.Exec(r.Ctx, `INSERT INTO Db(dbname) VALUES($1)`, req.DbName)
	case "DELETE":
		conn.Exec(r.Ctx, `DELETE FROM Db WHERE dbanme=$1 CASCADE;`, req.DbName)
	}
}

func (r *LlamaReceiver) SetupTables() error {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS db(id bigserial PRIMARY KEY, dbname TEXT)`)
	if err != nil {
		log.Println("[ERROR]: unable to create Db table : " + err.Error())
		return err
	}

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS Measurement (
		created_time TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		data JSONB,
		metric_name TEXT,
		database_id SERIAL,
		FOREIGN KEY (database_id) REFERENCES Db(id)
	);`)
	if err != nil {
		log.Println("[ERROR]: unable to create Measurement table : " + err.Error())
		return err
	}

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS Insights(
		insight_data TEXT, 
		database_id BIGSERIAL, 
		created_time TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		FOREIGN KEY (database_id) REFERENCES Db(id) 
	)`)
	if err != nil {
		log.Println("[ERROR]: unable to create Insigths table : " + err.Error())
		return err
	}

	return nil
}

func (r *LlamaReceiver) AddMeasurements(msg *api.MeasurementEnvelope) error {

	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	var id int
	// Try to fecth id
	err = conn.QueryRow(r.Ctx, `SELECT id FROM Db WHERE dbname='`+msg.DBName+`'`).Scan(&id)
	if err != nil {
		if err.Error() != "no rows in result set" {
			return err
		}
		// if not found, add database to table
		_, err := conn.Exec(r.Ctx, `INSERT INTO Db(dbname) VALUES('`+msg.DBName+`')`)
		if err != nil {
			return err
		}

		// Get new id
		err = conn.QueryRow(r.Ctx, `SELECT id FROM Db WHERE dbname='`+msg.DBName+`'`).Scan(&id)
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
	_, err = conn.Exec(r.Ctx, fmt.Sprintf(`INSERT INTO Measurement(data, database_id, metric_name) VALUES('%s', %d, '%s')`, string(jsonData), id, msg.MetricName))
	if err != nil {
		return err
	}

	return nil
}

func (r *LlamaReceiver) GetAllMeasurements(dbname string, metric_name string, context_size uint) ([]MeasurementsData, error) {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return nil, errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	query := fmt.Sprintf("SELECT database_id, metric_name, data FROM Measurement INNER JOIN Db ON Measurement.database_id = Db.id WHERE Db.dbname = '%s' ORDER BY created_time DESC LIMIT %d", dbname, context_size)
	rows, err := conn.Query(r.Ctx, query)
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
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		log.Println("[ERROR]: unable to acquire new connection")
		return -1
	}
	defer conn.Release()

	// Get id of database with name = dbname
	id := 0
	query := fmt.Sprintf(`SELECT id FROM Db where dbname='%s'`, dbname)
	err = conn.QueryRow(r.Ctx, query).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func (r *LlamaReceiver) AddInsights(dbid int, insights string) error {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	// Insert model response in table insights
	query := `INSERT INTO Insights(database_id, insight_data) VALUES($1, $2)`

	// Execute the query with parameters
	_, err = conn.Exec(r.Ctx, query, dbid, insights)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}

func (r *LlamaReceiver) GenerateInsights(msg api.MeasurementEnvelope) error {
	final_msg, err := r.PreparePrompt(msg.DBName, msg.MetricName)
	if err != nil {
		return err
	}

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
			log.Println(err)
			return
		}

		model_response += response.Message.Content
		log.Println(model_response)
	}

	var params *talkative.ChatParams = nil

	log.Println("Working on metrics....")
	// The chat message to send
	message := talkative.ChatMessage{
		Role:    talkative.USER, // Initiate the chat as a user
		Content: final_msg,
	}

	done, err := r.Client.PlainChat(model, callback, params, message)

	if err != nil {
		// Unable to start chat
		return errors.New("unable to send message to client. Please check if your ollama instance is up and running")
	}

	<-done // wait for the chat to complete
	log.Println("Completed ->", model_response)
	id := r.GetDbID(msg.DBName)
	if id == -1 {
		return errors.New("unable to find database in records")
	}

	err = r.AddInsights(id, model_response)
	if err != nil {
		return errors.New("unable to add new insights")
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

	// Mapping to batch measurements according to epoch time
	err := r.AddMeasurements(msg)
	if err != nil {
		return err
	}

	log.Println("[INFO]: Inserted entry into database")
	log.Println("[INFO]: Adding to batch")

	epochTime := msg.Data[0]["epoch_ns"]

	if epochTime == nil {
		*logMsg = "epoch time not present! assigning one ourselves"
		epochTimeStr := strconv.FormatInt(time.Now().Unix(), 10)
		epochTime = epochTimeStr
	} else {
		epochTime = strconv.FormatInt(epochTime.(int64), 10)
	}

	// Append msg to the appropriate key in MsmtBatch
	r.MsmtBatch[epochTime.(string)] = append(r.MsmtBatch[epochTime.(string)], msg)
	r.MsCount += 1

	// Process metrics if batch size acheived
	if r.MsCount == r.BatchSize {
		// Generate insights for measurements of batch set
		for _, v := range r.MsmtBatch {
			for _, val := range v {
				go func(val *api.MeasurementEnvelope) {
					r.mu.Lock()
					defer r.mu.Unlock()
					r.GenerateInsights(*val)
				}(val)
			}
		}

		// Delete all entries
		r.mu.Lock()
		log.Println("[INFO] : Removing old entries.....")
		for k := range r.MsmtBatch {
			delete(r.MsmtBatch, k)
		}
		log.Println("[INFO]: Removed entries successfully!")
		r.mu.Unlock()

		r.MsCount = 0
	}

	return nil
}
