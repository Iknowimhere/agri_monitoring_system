# 🌱 Agricultural Data Pipeline

A robust Express.js-based agricultural sensor data processing pipeline with Parquet file support, designed for real-time agricultural monitoring and analytics.

## ✨ Features

- **🔄 Real-time Data Processing**: Automated ingestion, transformation, and validation
- **📊 Parquet File Support**: Native support for reading and writing Parquet files
- **🔍 Data Validation**: Comprehensive quality checks and anomaly detection
- **📈 RESTful API**: Complete REST API with Swagger documentation
- **🐳 Docker Ready**: Fully containerized with development and production configurations
- **✅ 100% Test Coverage**: Comprehensive test suite with Jest
- **📝 Structured Logging**: Winston-based logging with rotation
- **🔒 Security**: Helmet, CORS, and rate limiting

## 🚀 Quick Start

### Prerequisites

- Node.js 18+ or Docker
- npm 8+

### Local Development

```bash
# Clone the repository
git clone https://github.com/satsure/agricultural-data-pipeline.git
cd agricultural-data-pipeline

# Install dependencies
npm install

# Start development server
npm run dev

# Run tests
npm test
```

### Docker Development

```bash
# Build and run with Docker Compose
npm run docker:dev

# View logs
npm run docker:logs

# Stop containers
npm run docker:stop
```

## 📊 API Endpoints

- **Health Check**: `GET /health`
- **API Documentation**: `GET /api-docs`
- **Data Query**: `GET /api/data/query`
- **Pipeline Management**: `POST /api/pipeline/run`
- **Reports**: `GET /api/reports`

Visit `http://localhost:3000/api-docs` for complete API documentation.

## 🐳 Docker Deployment

### Development

```bash
npm run docker:dev
```

### Production

```bash
npm run docker:prod
```

The production setup includes:

- Nginx reverse proxy
- Health checks
- Resource limits
- Volume persistence

## 📁 Project Structure

```
├── src/
│   ├── controllers/     # API request handlers
│   ├── services/        # Business logic and data processing
│   ├── routes/          # API route definitions
│   ├── middleware/      # Express middleware
│   ├── config/          # Configuration files
│   └── utils/           # Utility functions
├── tests/               # Test suites
├── data/                # Data directories
├── public/              # Static files
├── logs/                # Application logs
└── docker-compose.yml   # Container orchestration
```

## 🧪 Testing

```bash
# Run all tests
npm test

# Watch mode
npm run test:watch

# Coverage report
npm run test:coverage
```

## 📝 Data Formats

### Supported Input Formats

- **JSON**: Structured sensor data
- **Parquet**: Columnar data format for analytics
- **CSV**: Comma-separated values

### Sample Data Structure

```json
{
  "sensor_id": "Field-A-temperature-01",
  "timestamp": "2025-07-16T16:20:30.000Z",
  "reading_type": "temperature",
  "value": 25.5,
  "unit": "°C",
  "location": {
    "field": "Field-A",
    "latitude": 28.6139,
    "longitude": 77.209
  }
}
```

## 🔧 Configuration

Environment variables are configured in `.env`:

```env
NODE_ENV=development
PORT=3000
LOG_LEVEL=info
```

## 📈 Performance

- **Parquet Processing**: Optimized for large datasets
- **Memory Management**: Efficient streaming for large files
- **Caching**: In-memory caching for frequently accessed data
- **Rate Limiting**: Protection against API abuse

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Umashankar** - Agricultural Data Systems Specialist

---

**🌱 Built for modern agriculture with precision and performance in mind.**
