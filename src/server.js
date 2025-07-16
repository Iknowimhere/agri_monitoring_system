#!/usr/bin/env node

/**
 * Agricultural Data Pipeline Server Startup
 * Initializes DuckDB and starts the Express server
 */

const { app, initializeDatabase } = require('./app');
const logger = require('./utils/logger');
const config = require('./config/config');

const PORT = config.server.port;

async function startServer() {
  try {
    // Initialize database first
    logger.info('Starting Agricultural Data Pipeline Server...');
    await initializeDatabase();
    
    // Start the Express server
    const server = app.listen(PORT, () => {
      logger.info(`🚀 Server is running on port ${PORT}`, {
        port: PORT,
        environment: config.server.env,
        databaseInitialized: true
      });
      
      logger.info('Available endpoints:', {
        health: `http://localhost:${PORT}/health`,
        api: `http://localhost:${PORT}/api`,
        docs: `http://localhost:${PORT}/api-docs`
      });
    });

    // Graceful shutdown handling
    process.on('SIGTERM', async () => {
      logger.info('SIGTERM signal received: closing HTTP server');
      
      server.close(() => {
        logger.info('HTTP server closed');
        process.exit(0);
      });
    });

    process.on('SIGINT', async () => {
      logger.info('SIGINT signal received: closing HTTP server');
      
      server.close(() => {
        logger.info('HTTP server closed');
        process.exit(0);
      });
    });

  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Start the server
startServer();
