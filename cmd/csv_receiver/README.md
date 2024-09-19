# CSV Receiver

The CSV Receiver is a service for collecting and storing PostgreSQL metric data in CSV format. It writes incoming metrics to organized CSV files in a specified directory structure. Each database has its own directory and all the metrics are stored in separate csv files.

## Features

- **CSV Storage**: Metrics are saved in CSV files, organized by database and metric names.
- **Dynamic Folder Creation**: Automatically creates the necessary folders for each database and metric.
- **Error Handling**: Robust error handling for file operations.

## Usage
```bash
go run ./cmd/csv_receiver --port=<port_number_for_sink> --rootFolder=<location_on_disk>
```

## CSV Structure

The CSV files will be stored in the following structure:

Database Folder
  ├── Metric1.csv
  └── Metric2.csv