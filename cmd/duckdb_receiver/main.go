package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	// port := flag.String("port", "-1", "Specify the port where you want you sink to receive the measaurements on.")
	// // StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	// flag.Parse()

	// if *port == "-1" {
	// 	log.Println("[ERROR]: No Port Specified")
	// 	return
	// }

	dbr, err := NewDBDuckReceiver("test_metrics.duckdb")
	if err != nil {
		log.Fatal(err)
	}
	// rows, err := dbr.Conn.Query("SELECT * FROM measurements")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()

	// for rows.Next() {
	// 	var dbName, metricName string
	// 	var data, customTags, metricDef map[string]interface{} // envelope  data
	// 	var timestamp time.Time
	// 	err := rows.Scan(&dbName, &metricName, &data, &customTags, &metricDef, &timestamp)
	// 	if err != nil {
	// 		log.Printf("[-] Error at row: %v", err)
	// 		continue
	// 	}
	// 	fmt.Printf("DB: %s, Metric: %s, Time: %s\n", dbName, metricName, timestamp)

	// }
	rpc.RegisterName("Receiver", dbr) // Primary Receiver
	log.Println("[INFO]: Registered Receiver")
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "0.0.0.0:9876")

	if err != nil {
		log.Println(err)
	}

	http.Serve(listener, nil)

}
