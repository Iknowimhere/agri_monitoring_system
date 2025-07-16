const winston = require('winston');
const path = require('path');
const config = require('../config/config');

// Ensure logs directory exists
const fs = require('fs');
if (!fs.existsSync(config.paths.logs)) {
  fs.mkdirSync(config.paths.logs, { recursive: true });
}

const logger = winston.createLogger({
  level: config.logging.level,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    config.logging.format === 'json' 
      ? winston.format.json()
      : winston.format.simple()
  ),
  defaultMeta: { service: 'agricultural-pipeline-api' },
  transports: [
    // File transport
    new winston.transports.File({
      filename: path.join(config.paths.logs, 'error.log'),
      level: 'error'
    }),
    new winston.transports.File({
      filename: path.join(config.paths.logs, 'combined.log')
    })
  ]
});

// Add console transport in development
if (config.server.env !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    )
  }));
}

module.exports = logger;
