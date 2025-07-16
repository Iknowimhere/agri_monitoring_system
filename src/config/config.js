require('dotenv').config();
const yaml = require('yaml');
const fs = require('fs');
const path = require('path');

// Load YAML configuration
let yamlConfig = {};
try {
  const configFile = fs.readFileSync(path.join(__dirname, '../../config.yaml'), 'utf8');
  yamlConfig = yaml.parse(configFile);
} catch (error) {
  console.warn('Could not load config.yaml, using defaults');
}

const config = {
  server: {
    port: process.env.PORT || 3000,
    env: process.env.NODE_ENV || 'development'
  },
  
  api: {
    rateLimitWindowMs: parseInt(process.env.API_RATE_LIMIT_WINDOW_MS) || 900000, // 15 minutes
    rateLimitMaxRequests: parseInt(process.env.API_RATE_LIMIT_MAX_REQUESTS) || 100
  },
  
  database: {
    sqlitePath: process.env.SQLITE_PATH || 'data/pipeline.sqlite'
  },
  
  paths: {
    rawData: process.env.RAW_DATA_PATH || yamlConfig.ingestion?.raw_data_path || 'data/raw',
    processedData: process.env.PROCESSED_DATA_PATH || yamlConfig.storage?.processed_path || 'data/processed',
    checkpoints: process.env.CHECKPOINT_PATH || 'data/checkpoints',
    reports: process.env.REPORTS_PATH || yamlConfig.validation?.output_path || 'data/reports',
    logs: process.env.LOGS_PATH || 'logs'
  },
  
  processing: {
    maxFileSizeMB: parseInt(process.env.MAX_FILE_SIZE_MB) || 100,
    batchSize: parseInt(process.env.BATCH_SIZE) || 10000
  },
  
  // Merge with YAML config
  ingestion: yamlConfig.ingestion || {},
  transformation: yamlConfig.transformation || {},
  validation: yamlConfig.validation || {},
  storage: yamlConfig.storage || {},
  
  logging: {
    level: process.env.LOG_LEVEL || yamlConfig.logging?.level || 'info',
    format: yamlConfig.logging?.format || 'json'
  }
};

module.exports = config;
