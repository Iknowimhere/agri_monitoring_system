const logger = require('../utils/logger');
const DuckDBService = require('./duckDBService');
const config = require('../config/config');

class DuckDBSingleton {
  constructor() {
    this.instance = null;
    this.initializationPromise = null;
    this.isInitializing = false;
  }

  async getInstance() {
    if (this.instance) {
      return this.instance;
    }

    if (this.isInitializing) {
      // Wait for the current initialization to complete
      await this.initializationPromise;
      return this.instance;
    }

    this.isInitializing = true;
    this.initializationPromise = this.initializeInstance();
    
    try {
      await this.initializationPromise;
      return this.instance;
    } finally {
      this.isInitializing = false;
    }
  }

  async initializeInstance() {
    try {
      logger.info('DuckDBSingleton: Initializing single DuckDB instance...');
      
      this.instance = new DuckDBService(config);
      const result = await this.instance.initialize();
      
      if (result.success) {
        logger.info(`DuckDBSingleton: Initialized successfully in ${result.mode} mode`);
      } else {
        logger.error('DuckDBSingleton: Initialization failed');
      }
      
      return this.instance;
    } catch (error) {
      logger.error('DuckDBSingleton: Failed to initialize:', error);
      this.instance = null;
      throw error;
    }
  }

  async close() {
    if (this.instance) {
      await this.instance.close();
      this.instance = null;
      this.initializationPromise = null;
      this.isInitializing = false;
    }
  }
}

// Export a single instance
const duckDBSingleton = new DuckDBSingleton();

module.exports = duckDBSingleton;