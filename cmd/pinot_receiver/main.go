package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	// Important Flags
	port := flag.String("port", "", "Specify the port where you want your sink to receive the measurements on. Required.")
	pinotControllerURL := flag.String("pinotController", "", "URL for the Pinot controller. Required (e.g., http://localhost:9000).")
	configDir := flag.String("configDir", "./config", "Directory containing Pinot schema and table config files")
	flag.Parse()

	// Validate required parameters
	if *port == "" {
		log.Println("[ERROR]: No Port Specified (--port)")
		flag.Usage()
		return
	}

	if *pinotControllerURL == "" {
		log.Println("[ERROR]: No Pinot Controller URL Specified (--pinotController)")
		flag.Usage()
		return
	}

	// Simple URL fix - handle port-only case and add http:// if needed
	if strings.Contains(*pinotControllerURL, ".") == false && strings.Contains(*pinotControllerURL, ":") == false {
		// Just a port number
		*pinotControllerURL = "http://localhost:" + *pinotControllerURL
	} else if !strings.HasPrefix(*pinotControllerURL, "http") {
		// Has host but no protocol
		*pinotControllerURL = "http://" + *pinotControllerURL
	}
	log.Printf("[INFO]: Using Pinot controller URL: %s", *pinotControllerURL)

	// Verify config directory exists
	if _, err := os.Stat(*configDir); os.IsNotExist(err) {
		log.Fatalf("[ERROR]: Config directory %s does not exist.", *configDir)
		return
	}

	// Verify schema.json exists and get table name
	schemaPath := filepath.Join(*configDir, "schema.json")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		log.Fatalf("[ERROR]: Schema file %s does not exist.", schemaPath)
		return
	}

	// Extract table name from schema.json
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		log.Fatalf("[ERROR]: Failed to read schema file: %v", err)
		return
	}

	var schemaConfig map[string]interface{}
	if err := json.Unmarshal(schemaData, &schemaConfig); err != nil {
		log.Fatalf("[ERROR]: Failed to parse schema file: %v", err)
		return
	}

	tableName, ok := schemaConfig["schemaName"].(string)
	if !ok || tableName == "" {
		log.Fatalf("[ERROR]: Invalid or missing schemaName in schema.json")
		return
	}

	log.Printf("[INFO]: Using table name from schema: %s", tableName)

	// Initialize Pinot receiver
	server, err := NewPinotReceiver(*pinotControllerURL, tableName, *configDir)
	if err != nil {
		log.Fatalf("[ERROR]: Failed to initialize Pinot receiver: %v", err)
		return
	}

	log.Println("[INFO]: Pinot Receiver Initialized")

	if err := sinks.Listen(server, *port); err != nil {
		log.Fatal(err)
	}
}
