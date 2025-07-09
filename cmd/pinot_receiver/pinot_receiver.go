package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

// PinotReceiver handles sending metrics to a Pinot cluster
type PinotReceiver struct {
	ControllerURL string // URL to Pinot controller
	TableName     string // Name of the table in Pinot
	ConfigDir     string // Directory containing schema and table config
	Client        *http.Client
	sinks.SyncMetricHandler
}

func NewPinotReceiver(controllerURL, tableName, configDir string) (*PinotReceiver, error) {
	receiver := &PinotReceiver{
		ControllerURL:     controllerURL,
		TableName:         tableName,
		ConfigDir:         configDir,
		Client:            &http.Client{Timeout: 30 * time.Second},
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	// Ensure config directory exists
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("config directory %s does not exist", configDir)
	}

	// Initialize schema and table
	if err := receiver.initializePinotTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize Pinot table: %v", err)
	}

	go receiver.HandleSyncMetric()

	return receiver, nil
}

// initializePinotTable creates the schema and table in Pinot if they don't exist
func (r *PinotReceiver) initializePinotTable() error {
	// Check for required schema and table config files
	schemaConfigPath := filepath.Join(r.ConfigDir, "schema.json")
	tableConfigPath := filepath.Join(r.ConfigDir, "table.json")

	// Verify schema file exists
	if _, err := os.Stat(schemaConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("schema config file not found at %s", schemaConfigPath)
	}
	log.Printf("[INFO]: Using schema config at %s", schemaConfigPath)

	// Verify table file exists
	if _, err := os.Stat(tableConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("table config file not found at %s", tableConfigPath)
	}
	log.Printf("[INFO]: Using table config at %s", tableConfigPath)

	// Verify schema and table configs match
	schemaData, err := os.ReadFile(schemaConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read schema config: %v", err)
	}

	tableData, err := os.ReadFile(tableConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read table config: %v", err)
	}

	var schemaConfig map[string]interface{}
	if err := json.Unmarshal(schemaData, &schemaConfig); err != nil {
		return fmt.Errorf("failed to parse schema config: %v", err)
	}

	var tableConfig map[string]interface{}
	if err := json.Unmarshal(tableData, &tableConfig); err != nil {
		return fmt.Errorf("failed to parse table config: %v", err)
	}

	schemaName := schemaConfig["schemaName"].(string)
	tableName := tableConfig["tableName"].(string)

	if schemaName != tableName {
		return fmt.Errorf("schema name (%s) does not match table name (%s)", schemaName, tableName)
	}

	log.Printf("[INFO]: Verified schema name (%s) matches table name (%s)", schemaName, tableName)

	// Upload schema to Pinot
	if err := r.uploadSchema(schemaConfigPath); err != nil {
		return fmt.Errorf("failed to upload schema: %v", err)
	}

	// Create table in Pinot
	if err := r.createTable(tableConfigPath); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	log.Println("[INFO]: Pinot schema and table initialized successfully")
	return nil
}

// uploadSchema uploads the schema to Pinot controller
func (r *PinotReceiver) uploadSchema(schemaPath string) error {
	// Read schema file
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	// Log the exact schema being sent to Pinot
	log.Printf("[DEBUG]: Schema being sent to Pinot: %s", string(schemaData))

	// URL for schema upload
	url := fmt.Sprintf("%s/schemas", r.ControllerURL)
	log.Printf("[DEBUG]: Sending schema to: %s", url)

	// Create request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(schemaData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() {_ = resp.Body.Close()}()

	// Check response
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		// Log full response for debugging
		log.Printf("[ERROR]: Pinot schema upload response: %s", string(body))

		// If error message indicates schema already exists, that's fine
		if bytes.Contains(body, []byte("already exists")) {
			log.Printf("[INFO]: Schema %s already exists", r.TableName)
			return nil
		}
		return fmt.Errorf("failed to upload schema: %s - %s", resp.Status, string(body))
	}

	return nil
}

// createTable creates a table in Pinot using the table config
func (r *PinotReceiver) createTable(tableConfigPath string) error {
	// Read table config file
	tableData, err := os.ReadFile(tableConfigPath)
	if err != nil {
		return err
	}

	// Log the exact table config being sent to Pinot
	log.Printf("[DEBUG]: Table configuration being sent to Pinot: %s", string(tableData))

	// URL for table creation
	url := fmt.Sprintf("%s/tables", r.ControllerURL)
	log.Printf("[DEBUG]: Sending table creation request to: %s", url)

	// Create request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(tableData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() {_ = resp.Body.Close()}()

	// Check response (table might already exist, which is fine)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		// Log full response for debugging
		log.Printf("[ERROR]: Pinot table creation response: %s", string(body))

		// If error message indicates table already exists, that's fine
		if bytes.Contains(body, []byte("already exists")) {
			log.Printf("[INFO]: Table %s already exists", r.TableName)
			return nil
		}
		return fmt.Errorf("failed to create table: %s - %s", resp.Status, string(body))
	}

	return nil
}

func (r *PinotReceiver) insertData(dbName, metricName, data, customTags string) error {
	// Format data for Pinot ingestion
	ingestionData := map[string]interface{}{
		"dbname":      dbName,
		"metric_name": metricName,
		"data":        data,
		"custom_tags": customTags,
		"timestamp":   time.Now().UnixMilli(),
	}

	// Convert to JSON
	jsonData, err := json.Marshal([]map[string]interface{}{ingestionData})
	if err != nil {
		return err
	}

	// Create a file with the JSON data
	tempFile, err := os.CreateTemp("", "pinot-ingestion-*.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer func() {_ = os.Remove(tempFile.Name())}()
	defer func() {_ = tempFile.Close()}()

	if _, err := tempFile.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write to temp file: %v", err)
	}
	_ = tempFile.Close() // Close the file before reading it

	// Create a buffer to hold the multipart form data
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)

	// Open the temp file for reading
	fileReader, err := os.Open(tempFile.Name())
	if err != nil {
		return fmt.Errorf("failed to open temp file: %v", err)
	}
	defer func() {_ = fileReader.Close()}()

	// Add the file to the form
	filePart, err := writer.CreateFormFile("file", "data.json")
	if err != nil {
		return fmt.Errorf("failed to create form file: %v", err)
	}
	if _, err := io.Copy(filePart, fileReader); err != nil {
		return fmt.Errorf("failed to copy file data: %v", err)
	}

	// Close the writer to finalize the form data
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close multipart writer: %v", err)
	}

	// URL with batchConfigMapStr parameter for JSON input format
	batchConfig := url.QueryEscape(`{"inputFormat":"json"}`)
	url := fmt.Sprintf("%s/ingestFromFile?tableNameWithType=%s_OFFLINE&batchConfigMapStr=%s",
		r.ControllerURL, r.TableName, batchConfig)
	log.Printf("[DEBUG] Sending to URL: %s", url)

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, &buffer)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set the content type header
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Send the request
	resp, err := r.Client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer func() {_ = resp.Body.Close()}()

	// Read and log the response
	body, _ := io.ReadAll(resp.Body)
	log.Printf("[DEBUG] Ingestion response: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to insert data: %s - %s", resp.Status, string(body))
	}

	log.Println("[INFO]: Inserted data successfully")
	return nil
}

func (r *PinotReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return nil, err
	}

	customTagsJSON, err := sinks.GetJson(msg.GetCustomTags())
	if err != nil {
		return nil, err
	}

	for _, measurement := range msg.GetData() {
		measurementJSON, err := sinks.GetJson(measurement)
		if err != nil {
			return nil, err
		}

		err = r.insertData(msg.GetDBName(), msg.GetMetricName(), measurementJSON, customTagsJSON)
		if err != nil {
			logMsg := fmt.Sprintf("error inserting data: %v", err)
			return nil, errors.New(logMsg)
		}
	}

	logMsg := "[INFO]: Successfully inserted batch at : " + time.Now().String()
	log.Println(logMsg)
	return &pb.Reply{Logmsg: logMsg}, nil
}