const logger = require('../utils/logger');
const IngestionService = require('./ingestion/ingestionService');
const TransformationService = require('./transformation/transformationService');
const ValidationService = require('./validation/validationService');
const StorageService = require('./storage/storageService');
const FileUtils = require('../utils/fileUtils');
const DateUtils = require('../utils/dateUtils');
const config = require('../config/config');
const path = require('path');
const fs = require('fs');

class PipelineService {
  constructor() {
    this.status = {
      status: 'idle', // idle, running, completed, failed
      stage: null,
      progress: 0,
      startTime: null,
      endTime: null,
      statistics: {
        filesProcessed: 0,
        recordsProcessed: 0,
        recordsSkipped: 0,
        errors: 0
      }
    };
    this.isRunning = false;
    this.shouldStop = false;
  }

  async getStatus() {
    return { ...this.status };
  }

  async runFullPipeline(options = {}) {
    const { startDate, endDate, forceReprocess = false } = options;
    
    if (this.isRunning) {
      throw new Error('Pipeline is already running');
    }

    try {
      this.isRunning = true;
      this.shouldStop = false;
      this.status = {
        status: 'running',
        stage: 'initialization',
        progress: 0,
        startTime: DateUtils.nowIST().toISOString(),
        endTime: null,
        statistics: {
          filesProcessed: 0,
          recordsProcessed: 0,
          recordsSkipped: 0,
          errors: 0
        }
      };

      logger.info('Starting full pipeline execution', { startDate, endDate, forceReprocess });

      // Stage 1: Ingestion
      this.status.stage = 'ingestion';
      this.status.progress = 10;
      const ingestionResult = await this.runIngestion({ startDate, endDate });
      
      if (this.shouldStop) {
        throw new Error('Pipeline stopped by user');
      }

      // Stage 2: Transformation
      this.status.stage = 'transformation';
      this.status.progress = 40;
      const transformationResult = await this.runTransformation();
      
      if (this.shouldStop) {
        throw new Error('Pipeline stopped by user');
      }

      // Stage 3: Validation
      this.status.stage = 'validation';
      this.status.progress = 70;
      const validationResult = await this.runValidation();
      
      if (this.shouldStop) {
        throw new Error('Pipeline stopped by user');
      }

      // Stage 4: Storage
      this.status.stage = 'storage';
      this.status.progress = 90;
      const storageResult = await this.runStorage();

      // Complete
      this.status.status = 'completed';
      this.status.stage = 'completed';
      this.status.progress = 100;
      this.status.endTime = DateUtils.nowIST().toISOString();

      // Aggregate statistics
      this.status.statistics = {
        filesProcessed: ingestionResult.filesProcessed + storageResult.filesWritten,
        recordsProcessed: transformationResult.recordsProcessed,
        recordsSkipped: transformationResult.recordsSkipped,
        errors: ingestionResult.errors + transformationResult.errors + validationResult.errors
      };

      logger.info('Pipeline completed successfully', this.status.statistics);

      return this.status;
    } catch (error) {
      this.status.status = 'failed';
      this.status.endTime = DateUtils.nowIST().toISOString();
      logger.error('Pipeline execution failed:', error);
      throw error;
    } finally {
      this.isRunning = false;
      this.shouldStop = false;
    }
  }

  async runIngestion(options = {}) {
    logger.info('Starting data ingestion stage');
    
    try {
      const ingestionService = new IngestionService(config);
      const result = await ingestionService.ingestData(options);
      
      logger.info('Ingestion completed', result);
      return result;
    } catch (error) {
      logger.error('Ingestion failed:', error);
      throw error;
    }
  }

  async runTransformation() {
    logger.info('Starting data transformation stage');
    
    try {
      const transformationService = new TransformationService(config);
      const result = await transformationService.transformData();
      
      logger.info('Transformation completed', result);
      return result;
    } catch (error) {
      logger.error('Transformation failed:', error);
      throw error;
    }
  }

  async runValidation() {
    logger.info('Starting data validation stage');
    
    try {
      const validationService = new ValidationService(config);
      const result = await validationService.validateData();
      
      logger.info('Validation completed', result);
      return result;
    } catch (error) {
      logger.error('Validation failed:', error);
      throw error;
    }
  }

  async runStorage() {
    logger.info('Starting data storage stage');
    
    try {
      const storageService = new StorageService(config);
      const result = await storageService.storeData();
      
      logger.info('Storage completed', result);
      return result;
    } catch (error) {
      logger.error('Storage failed:', error);
      throw error;
    }
  }

  async stopPipeline() {
    if (!this.isRunning) {
      return { success: false, message: 'No pipeline is currently running' };
    }

    this.shouldStop = true;
    this.status.status = 'stopped';
    this.status.endTime = DateUtils.nowIST().toISOString();
    
    logger.info('Pipeline stop requested');
    
    return { 
      success: true, 
      stoppedAt: this.status.endTime 
    };
  }

  async getLogs(options = {}) {
    const { lines = 100, level } = options;
    
    try {
      const logFile = path.join(config.paths.logs, 'combined.log');
      
      if (!(await FileUtils.fileExists(logFile))) {
        return [];
      }

      const logContent = await fs.promises.readFile(logFile, 'utf8');
      const logLines = logContent.split('\n').filter(line => line.trim());
      
      // Filter by level if specified
      let filteredLogs = logLines;
      if (level) {
        filteredLogs = logLines.filter(line => {
          try {
            const logEntry = JSON.parse(line);
            return logEntry.level === level;
          } catch {
            return line.toLowerCase().includes(level.toLowerCase());
          }
        });
      }

      // Return last N lines
      return filteredLogs.slice(-lines).map(line => {
        try {
          return JSON.parse(line);
        } catch {
          return { message: line, timestamp: new Date().toISOString() };
        }
      });
    } catch (error) {
      logger.error('Failed to read logs:', error);
      return [];
    }
  }
}

// Export singleton instance
module.exports = new PipelineService();
