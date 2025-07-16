# Agricultural Data Pipeline API

A production-grade web API built with Express.js that provides endpoints for ingesting, transforming, validating, and storing agricultural sensor data from multiple sources using DuckDB for efficient processing.

## Overview

This pipeline processes agricultural sensor data that tracks:

- Soil moisture
- Temperature
- Humidity
- Light intensity
- Battery levels

The pipeline delivers clean, enriched, and query-optimized data for downstream analytics through RESTful API endpoints.

## Architecture

```
data/raw/           # Raw Parquet files (daily)
    └── YYYY-MM-DD.parquet

data/processed/     # Cleaned and transformed data
    └── partitioned by date/sensor_id

src/
    ├── routes/         # Express.js API routes
    ├── controllers/    # Route controllers
    ├── services/       # Business logic services
    │   ├── ingestion/     # Data ingestion components
    │   ├── transformation/# Data cleaning and transformation
    │   ├── validation/    # Data quality validation
    │   └── storage/       # Data loading and storage
    ├── middleware/     # Express middleware
    ├── models/         # Data models and schemas
    └── utils/          # Common utilities

tests/              # Unit and integration tests
public/             # Static files (dashboard)
logs/               # Pipeline logs
```

## Features

### Data Ingestion

- Modular ingestion component for daily Parquet files
- Incremental loading with timestamp-based checkpointing
- DuckDB-powered schema inspection and validation
- Comprehensive error handling and logging

### Data Transformation

- Duplicate removal and missing value handling
- Outlier detection using z-score analysis
- Derived fields: daily averages, 7-day rolling averages
- Anomaly detection with configurable thresholds
- Calibration normalization
- UTC+5:30 timezone conversion

### Data Quality Validation

- Type validation and range checking
- Gap detection in hourly data
- Comprehensive data profiling
- Quality reports in CSV format

### Data Storage

- Optimized Parquet format with compression
- Date and sensor_id partitioning
- Column-oriented storage for analytical queries

## Setup & Installation

### Prerequisites

- Node.js 16.0+ and npm 8.0+
- Docker (optional)

### Local Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd agricultural-data-pipeline
```

2. Install dependencies:

```bash
npm install
```

3. Copy environment variables:

```bash
copy .env.example .env  # Windows
# cp .env.example .env  # Linux/Mac
```

4. Create necessary directories:

```bash
mkdir -p data/raw data/processed data/checkpoints data/reports logs
```

5. Start the development server:

```bash
npm run dev
```

6. Access the application:

- API: http://localhost:3000
- Dashboard: http://localhost:3000
- API Documentation: http://localhost:3000/api-docs

### Docker Setup

1. Build the Docker image:

```bash
docker build -t ag-pipeline-api .
```

2. Run the container:

```bash
docker run -p 3000:3000 -v ${PWD}/data:/app/data ag-pipeline-api
```

## API Endpoints

### Pipeline Management

- `GET /api/pipeline/status` - Get current pipeline status
- `POST /api/pipeline/run` - Start the complete pipeline
- `POST /api/pipeline/upload` - Upload raw data files
- `POST /api/pipeline/stop` - Stop running pipeline
- `GET /api/pipeline/logs` - Get pipeline execution logs

### Data Access

- `GET /api/data/query` - Query processed sensor data
- `GET /api/data/sensors` - Get list of all sensors
- `GET /api/data/sensors/{sensor_id}/summary` - Get sensor summary
- `GET /api/data/aggregations/daily` - Get daily aggregations
- `GET /api/data/anomalies` - Get anomalous readings
- `GET /api/data/export` - Export data in various formats

### Reports

- `GET /api/reports/quality` - Generate data quality report
- `GET /api/reports/processing` - Get processing statistics
- `GET /api/reports/coverage` - Get data coverage analysis
- `GET /api/reports/anomalies` - Get anomaly detection report
- `GET /api/reports/list` - List generated reports

### System

- `GET /health` - System health check
- `GET /api-docs` - Interactive API documentation

## Configuration

The API uses both environment variables (.env) and YAML configuration (config.yaml):

### Environment Variables (.env)

```env
PORT=3000
NODE_ENV=development
LOG_LEVEL=info
DUCKDB_PATH=data/pipeline.duckdb
RAW_DATA_PATH=data/raw
PROCESSED_DATA_PATH=data/processed
```

```yaml
ingestion:
  raw_data_path: 'data/raw'
  checkpoint_file: 'data/checkpoints/last_processed.txt'

transformation:
  calibration:
    temperature: { multiplier: 1.0, offset: 0.0 }
    humidity: { multiplier: 1.0, offset: 0.0 }
    soil_moisture: { multiplier: 1.0, offset: 0.0 }

  anomaly_thresholds:
    temperature: { min: -10, max: 50 }
    humidity: { min: 0, max: 100 }
    soil_moisture: { min: 0, max: 100 }
    light_intensity: { min: 0, max: 100000 }
    battery_level: { min: 0, max: 100 }

validation:
  output_path: 'data/reports'

storage:
  processed_path: 'data/processed'
  compression: 'snappy'
  partition_cols: ['date', 'sensor_id']
```

## Calibration & Anomaly Logic

### Calibration

The pipeline applies sensor-specific calibration using the formula:

```
calibrated_value = raw_value * multiplier + offset
```

### Anomaly Detection

- **Z-score method**: Values with |z-score| > 3 are flagged as statistical outliers
- **Range-based**: Values outside predefined ranges per reading_type are flagged
- **Time-series**: Significant deviations from 7-day rolling averages

## Data Quality Report

The pipeline generates comprehensive quality reports including:

- Missing value percentages by reading_type
- Anomalous reading percentages
- Time coverage gaps per sensor
- Schema validation results
- Processing statistics

Example report structure:

```csv
metric,reading_type,sensor_id,value,timestamp
missing_percentage,temperature,SENSOR_001,2.3,2023-06-01T10:00:00Z
anomaly_percentage,humidity,SENSOR_002,0.8,2023-06-01T10:00:00Z
gap_hours,soil_moisture,SENSOR_003,4,2023-06-01T10:00:00Z
```

## Usage Examples

### Starting the API Server:

```bash
npm start
# or for development with auto-restart
npm run dev
```

### Making API Calls:

```bash
# Check system health
curl http://localhost:3000/health

# Get pipeline status
curl http://localhost:3000/api/pipeline/status

# Start pipeline
curl -X POST http://localhost:3000/api/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"startDate": "2023-06-01", "endDate": "2023-06-03"}'

# Query sensor data
curl "http://localhost:3000/api/data/query?sensor_id=SENSOR_001&reading_type=temperature&limit=10"

# Generate quality report
curl http://localhost:3000/api/reports/quality
```

### Using the Web Dashboard:

1. Open http://localhost:3000 in your browser
2. View real-time pipeline status and system health
3. Start/stop pipeline operations
4. Generate and download reports
5. Monitor recent activity and logs

### Uploading Data Files:

```bash
# Upload Parquet files via API
curl -X POST http://localhost:3000/api/pipeline/upload \
  -F "files=@data/raw/2023-06-01.parquet" \
  -F "files=@data/raw/2023-06-02.parquet"
```

## Testing

Run the test suite:

```bash
# Run all tests
npm test

# Run tests with coverage
npm run test

# Run tests in watch mode
npm run test:watch

# Run linting
npm run lint

# Fix linting issues
npm run lint:fix
```

## Development

### Project Structure

```
src/
├── app.js              # Express application setup
├── config/             # Configuration management
├── routes/             # API route definitions
├── controllers/        # Route handlers
├── services/           # Business logic
├── middleware/         # Express middleware
├── models/             # Data models
└── utils/              # Utility functions

tests/                  # Test files
public/                 # Static web dashboard
data/                   # Data directories
logs/                   # Application logs
```

### Adding New Features

1. Define routes in `src/routes/`
2. Implement controllers in `src/controllers/`
3. Add business logic in `src/services/`
4. Write tests in `tests/`
5. Update API documentation

## Monitoring & Logging

The pipeline provides comprehensive logging:

- Ingestion statistics (files processed, records, errors)
- Transformation metrics (duplicates removed, outliers detected)
- Validation results (schema mismatches, quality scores)
- Performance metrics (processing time, memory usage)

Logs are structured in JSON format for easy parsing and monitoring.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
