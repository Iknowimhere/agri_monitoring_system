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

The pipeline provides comprehensive reporting capabilities with multiple access methods:

#### API Endpoints

- `GET /api/reports/quality` - Generate data quality report
- `GET /api/reports/processing` - Get processing statistics
- `GET /api/reports/coverage` - Get data coverage analysis
- `GET /api/reports/anomalies` - Get anomaly detection report
- `GET /api/reports/list` - List generated reports
- `GET /api/reports/{report_id}` - Download specific report by ID

#### Report Storage

Reports are automatically stored in multiple locations:

- **File Storage**: `data/reports/` directory
- **Report Types**:
  - `quality_*.json` - Data quality analysis reports
  - `summary_*.json` - Summary statistics reports
  - `analytics_*.json` - Analytics and insights reports
  - `processing_*.json` - Pipeline processing statistics
  - `custom_*.json` - Custom generated reports

#### Report Formats

- **JSON**: Default format for API responses
- **CSV**: Available for quality reports using `?format=csv` parameter
- **Direct Download**: Use report ID to download files

#### Access Methods

1. **Web Dashboard**: Visit `http://localhost` and click "📊 Generate Report"
2. **API Calls**: Direct HTTP requests to report endpoints
3. **File Access**: Direct access to stored report files
4. **PowerShell**: Use `Invoke-RestMethod` for Windows environments

#### Example Commands

```bash
# Generate quality report
curl http://localhost/api/reports/quality

# Get processing statistics
curl http://localhost/api/reports/processing

# List all reports
curl http://localhost/api/reports/list

# Download specific report
curl http://localhost/api/reports/quality_1234567890

# Generate CSV format quality report
curl "http://localhost/api/reports/quality?format=csv"
```

#### PowerShell Examples

```powershell
# Generate quality report
Invoke-RestMethod -Uri "http://localhost/api/reports/quality"

# Get processing report
Invoke-RestMethod -Uri "http://localhost/api/reports/processing"

# List all reports
Invoke-RestMethod -Uri "http://localhost/api/reports/list"
```

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

The pipeline generates comprehensive quality reports with detailed metrics and analysis:

### Report Content

- **Missing value percentages** by reading_type and sensor
- **Anomalous reading percentages** with statistical analysis
- **Time coverage gaps** per sensor with gap duration
- **Schema validation results** and data type compliance
- **Processing statistics** with performance metrics
- **Quality scores** and overall health assessments

### Report Types Generated

1. **Quality Reports** (`quality_*.json`)

   - Data completeness analysis
   - Accuracy rate calculations
   - Missing values and outlier detection
   - Overall quality scoring (0-100%)

2. **Processing Reports** (`processing_*.json`)

   - Records ingested, transformed, and stored
   - Processing time and performance metrics
   - Duplicates removed and outliers detected
   - Average records per second throughput

3. **Summary Reports** (`summary_*.json`)
   - Total sensors and data points
   - Active vs inactive sensor counts
   - Quality score summaries
   - Sensor-specific statistics

### Example Report Structure (CSV Format)

```csv
metric,reading_type,sensor_id,value,timestamp
missing_percentage,temperature,SENSOR_001,2.3,2023-06-01T10:00:00Z
anomaly_percentage,humidity,SENSOR_002,0.8,2023-06-01T10:00:00Z
gap_hours,soil_moisture,SENSOR_003,4,2023-06-01T10:00:00Z
quality_score,overall,ALL_SENSORS,87.5,2023-06-01T10:00:00Z
```

### Example Report Structure (JSON Format)

```json
{
  "report_id": "quality_1752739518864",
  "generated_at": "2025-07-17T08:05:18.893Z",
  "metadata": {
    "source": "duckdb",
    "file_path": "/reports/quality_1752739518864.json"
  },
  "quality_metrics": {
    "completeness": 95.2,
    "accuracy_rate": 87.5,
    "total_records": 150000,
    "missing_values": 1200,
    "outliers": 300
  },
  "summary": {
    "overall_quality_score": 87.5,
    "total_sensors": 3,
    "total_readings": 150000,
    "quality_issues": 1500
  }
}
```

### Real-time Report Generation

Reports are generated automatically and can be accessed immediately:

- **Automatic Storage**: All reports saved to `data/reports/` directory
- **Instant Access**: Available via API endpoints immediately after generation
- **Multiple Formats**: JSON (default) and CSV export options
- **Historical Archive**: All generated reports are preserved for analysis

## 🎥 Demo Video

Watch the complete pipeline in action:

[![Agricultural Data Pipeline Demo](https://img.shields.io/badge/▶️_Watch_Demo-blue?style=for-the-badge)](demo/pipeline-demo.mp4)

The demo showcases:

- **Data Ingestion**: Loading Parquet files and processing sensor data
- **Transformation**: Real-time data cleaning, calibration, and anomaly detection
- **Validation**: Quality checks and report generation
- **DuckDB Queries**: Interactive data analysis and aggregations

_Demo video shows the complete pipeline processing 10,000+ sensor readings in under 30 seconds_

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

# Generate processing report
curl http://localhost:3000/api/reports/processing

# List all generated reports
curl http://localhost:3000/api/reports/list

# Get anomaly detection report
curl http://localhost:3000/api/reports/anomalies

# Export quality report as CSV
curl "http://localhost:3000/api/reports/quality?format=csv"

# Download specific report by ID
curl http://localhost:3000/api/reports/quality_1752739518864
```

### PowerShell Examples:

```powershell
# Check pipeline status
Invoke-RestMethod -Uri "http://localhost/api/pipeline/status"

# Generate and view quality report
$qualityReport = Invoke-RestMethod -Uri "http://localhost/api/reports/quality"
$qualityReport | ConvertTo-Json -Depth 10

# Get processing statistics
$processingReport = Invoke-RestMethod -Uri "http://localhost/api/reports/processing"
Write-Output "Records Processed: $($processingReport.statistics.records_ingested)"

# List all available reports
$reportList = Invoke-RestMethod -Uri "http://localhost/api/reports/list"
Write-Output "Total Reports Generated: $($reportList.total_reports)"
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
