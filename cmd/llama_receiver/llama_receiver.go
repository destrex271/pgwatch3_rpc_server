package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
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
`

type LLamaReceiver struct {
	Client    *talkative.Client
	Context   string
	Ctx       context.Context
	ServerURI string
	ConnPool  *pgxpool.Pool
	MsmtBatch []*pb.MeasurementEnvelope
	BatchSize int
	mu sync.Mutex
	MsCount   int
	InsightsGenerationWg *sync.WaitGroup
	sinks.SyncMetricHandler
}

type MeasurementsData struct {
	metricName string
	data       string
}

func NewLLamaReceiver(LLamaServerURI string, pgURI string, ctx context.Context, batchSize int) (recv *LLamaReceiver, err error) {
	client, err := talkative.New(LLamaServerURI)
	if err != nil {
		log.Println("[ERROR]: unable to initialize llama client")
		return nil, err
	}

	// To use in insight generation to avoid any stuff
	pgxpool_config, err := pgxpool.ParseConfig(pgURI)
	if err != nil {
		log.Println("[ERROR]: unable to create pgx pool config")
		return nil, err
	}

	pgxpool_config.MaxConns = 25
	pgxpool_config.MaxConnLifetime = 15 * time.Minute
	pgxpool_config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, pgxpool_config)
	if err != nil {
		log.Println("[ERROR]: Unable to intialize conneciton pool.")
		return nil, err
	}

	recv = &LLamaReceiver{
		Client:            client,
		Context:           contextString,
		Ctx:               ctx,
		ServerURI:         LLamaServerURI,
		ConnPool:          pool,
		MsmtBatch:         make([]*pb.MeasurementEnvelope, 0, batchSize),
		BatchSize:         batchSize,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
		MsCount:           0,
		InsightsGenerationWg: &sync.WaitGroup{},
	}

	err = recv.SetupTables()
	if err != nil {
		log.Println("[ERROR]: unable to setup tables: ", err)
		return nil, err
	}

	go recv.HandleSyncMetric()

	return recv, nil
}

func (r *LLamaReceiver) HandleSyncMetric() {
	for {
		req, ok := r.GetSyncChannelContent()
		if !ok {
			// channel has been closed
			return
		}

		// Acquire connetion
		conn, err := r.ConnPool.Acquire(r.Ctx)
		if err != nil {
			log.Println("[ERROR]: Unable to acquire connection")
			return
		}
		defer conn.Release()

		switch req.Operation {
		case pb.SyncOp_AddOp:
			_, err = conn.Exec(r.Ctx, `INSERT INTO db(dbname) VALUES($1)`, req.GetDBName())
		case pb.SyncOp_DeleteOp:
			_, err = conn.Exec(r.Ctx, `DELETE FROM db WHERE dbanme=$1 CASCADE;`, req.GetDBName())
		}

		if err != nil {
			log.Printf("[ERROR] error handling LLama SyncMetric operation: %s", err)
		}
	}
}

func (r *LLamaReceiver) SetupTables() error {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS db(id BIGSERIAL PRIMARY KEY, dbname TEXT)`)
	if err != nil {
		log.Println("[ERROR]: unable to create db table : " + err.Error())
		return err
	}

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS measurements (
		created_at TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		data JSONB,
		metric_name TEXT,
		database_id SERIAL,
		FOREIGN KEY (database_id) REFERENCES db(id)
	);`)
	if err != nil {
		log.Println("[ERROR]: unable to create Measurement table : " + err.Error())
		return err
	}

	_, err = conn.Exec(r.Ctx, `CREATE TABLE IF NOT EXISTS insights(
		insight_data TEXT, 
		database_id BIGSERIAL, 
		created_at TIMESTAMP NOT NULL DEFAULT(NOW() AT TIME ZONE 'UTC'),
		FOREIGN KEY (database_id) REFERENCES db(id) 
	)`)
	if err != nil {
		log.Println("[ERROR]: unable to create Insigths table : " + err.Error())
		return err
	}

	return nil
}

func (r *LLamaReceiver) AddMeasurements(msg *pb.MeasurementEnvelope) error {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	var id int
	// Try to fetch id
	err = conn.QueryRow(r.Ctx, `SELECT id FROM db WHERE dbname=$1`, msg.GetDBName()).Scan(&id)
	if err != nil {
		if err.Error() != "no rows in result set" {
			return err
		}
		// if not found, add database to table
		_, err := conn.Exec(r.Ctx, `INSERT INTO db(dbname) VALUES($1)`, msg.GetDBName())
		if err != nil {
			return err
		}

		// Get new id
		err = conn.QueryRow(r.Ctx, `SELECT id FROM db WHERE dbname=$1`, msg.GetDBName()).Scan(&id)
		if err != nil {
			return err
		}
	}

	// Convert measurement to json
	jsonData := sinks.GetJson(msg.GetData())

	// insert measurements with current timestamp(default) into table Measurement
	_, err = conn.Exec(r.Ctx, `INSERT INTO measurements(data, database_id, metric_name) VALUES($1, $2, $3)`, jsonData, id, msg.GetMetricName())
	if err != nil {
		return err
	}

	return nil
}

func (r *LLamaReceiver) GetAllMeasurements(dbname string, metric_name string, context_size uint) ([]MeasurementsData, error) {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return nil, errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	query := "SELECT metric_name, data FROM measurements INNER JOIN db ON measurements.database_id = db.id WHERE db.dbname = $1 ORDER BY created_at DESC LIMIT $2"
	rows, err := conn.Query(r.Ctx, query, dbname, context_size)
	if err != nil {
		return nil, err
	}

	var data []MeasurementsData

	for rows.Next() {
		var cur_data MeasurementsData
		err = rows.Scan(&cur_data.metricName, &cur_data.data)
		if err != nil {
			log.Println(err)
			continue
		}
		data = append(data, cur_data)
	}

	return data, nil
}

func (r *LLamaReceiver) PreparePrompt(dbname string, metric_name string) (string, error) {
	all_measurements, err := r.GetAllMeasurements(dbname, metric_name, 10)
	if err != nil {
		return "", err
	}

	var data_string string
	for _, measurement := range all_measurements {
		data_string += "Metric Name -> " + measurement.metricName + ": " + measurement.data + ".\n"
	}

	final_msg := strings.ReplaceAll(r.Context, "{DATA}", data_string)
	return final_msg, nil
}

func (r *LLamaReceiver) GetDBID(dbname string) (int, error) {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		log.Println("[ERROR]: unable to acquire new connection")
		return 0, err 
	}
	defer conn.Release()

	// Get id of database with name = dbname
	id := 0
	query := `SELECT id FROM db where dbname=$1`
	err = conn.QueryRow(r.Ctx, query, dbname).Scan(&id)
	if err != nil {
		return 0, err 
	}
	return id, nil
}

func (r *LLamaReceiver) AddInsights(dbid int, insights string) error {
	conn, err := r.ConnPool.Acquire(r.Ctx)
	if err != nil {
		return errors.New("unable to acquire new connection")
	}
	defer conn.Release()

	// Insert model response in table insights
	query := `INSERT INTO insights(database_id, insight_data) VALUES($1, $2)`
	_, err = conn.Exec(r.Ctx, query, dbid, insights)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}

func (r *LLamaReceiver) GenerateInsights(msg *pb.MeasurementEnvelope) error {
	final_msg, err := r.PreparePrompt(msg.GetDBName(), msg.GetMetricName())
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
		return errors.New("unable to send message to client. Please check if your ollama instance is up and running")
	}

	<-done // wait for the chat to complete
	id, err := r.GetDBID(msg.GetDBName())
	if err != nil {
		return errors.New("unable to find database in records")
	}

	err = r.AddInsights(id, model_response)
	if err != nil {
		return errors.New("unable to add new insights")
	}

	return nil
}

func (r *LLamaReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return nil, err
	}

	// store measurement in pg database
	err := r.AddMeasurements(msg)
	if err != nil {
		return nil, err
	}

	log.Println("[INFO]: Inserted entry into database")
	log.Println("[INFO]: Adding entry to batch")

	// lock to avoid raceing of multiple pgwatch instances
	r.mu.Lock()
	r.MsmtBatch = append(r.MsmtBatch, msg)
	r.MsCount += 1

	if r.MsCount == r.BatchSize {
		// Generate insights for measurements of batch set
		for _, val := range r.MsmtBatch {
			r.InsightsGenerationWg.Add(1)
			go func(val *pb.MeasurementEnvelope) {
				defer r.InsightsGenerationWg.Done()
				err = r.GenerateInsights(val)
				if err != nil {
					log.Printf("Error Generating Insights: %v", err)
				}
			}(val)
		}

		log.Println("[INFO]: Flushing Batch")
		r.MsmtBatch = r.MsmtBatch[:0]
		r.MsCount = 0
	}
	r.mu.Unlock()

	return &pb.Reply{}, nil
}