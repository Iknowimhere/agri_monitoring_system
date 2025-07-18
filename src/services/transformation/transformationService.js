// DuckDB integration with fallback support
const logger = require('../../utils/logger');
const DuckDBService = require('../duckDBService');
const duckDBSingleton = require('../duckDBSingleton');
const buildToolsChecker = require('../../utils/buildToolsChecker');
const path = require('path');
const fs = require('fs');

class TransformationService {
  constructor(config) {
    this.config = config;
    this.duckDBService = null;
    this.initStorage();
  }

  async initStorage() {
    try {
      logger.info('Transformation service initialization - checking DuckDB availability');
      
      const dataDir = path.join(process.cwd(), 'data');
      const rawDir = path.join(dataDir, 'raw');
      const transformedDir = path.join(dataDir, 'transformed');
      
      [dataDir, rawDir, transformedDir].forEach(dir => {
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }
      });
      
      this.rawDataPath = rawDir;
      this.transformedDataPath = transformedDir;

      // Initialize DuckDB service using singleton
      this.duckDBService = await duckDBSingleton.getInstance();
      
      logger.info('Transformation storage initialized successfully', {
        duckdb: this.duckDBService.isAvailable,
        fallback: !this.duckDBService.isAvailable
      });
    } catch (error) {
      logger.error('Transformation storage initialization failed:', error);
      throw error;
    }
  }

  async transformData() {
    logger.info('Starting data transformation process');

    const stats = {
      recordsProcessed: 0,
      recordsSkipped: 0,
      errors: 0,
      storage: this.duckDBService?.isUsingDuckDB() ? 'duckdb' : 'fallback'
    };

    try {
      const files = await fs.promises.readdir(this.rawDataPath);
      const dataFiles = files.filter(f => f.endsWith('.json') || f.endsWith('.parquet'));
      
      if (dataFiles.length === 0) {
        logger.warn('No raw data files found for transformation');
        return stats;
      }

      let allTransformedData = [];

      for (const file of dataFiles) {
        try {
          const filePath = path.join(this.rawDataPath, file);
          let rawData;

          if (file.endsWith('.json')) {
            rawData = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
          } else if (file.endsWith('.parquet')) {
            // Use DataService to read Parquet files
            const DataService = require('../dataService');
            rawData = await DataService.readParquetFile(filePath);
          }
          
          const transformedData = await this.performTransformations(rawData);
          
          // Store in DuckDB or fallback
          if (this.duckDBService) {
            await this.duckDBService.insertData(transformedData);
          }

          // Also save to file for backup
          const outputFile = path.join(this.transformedDataPath, `transformed_${file.replace('.parquet', '.json')}`);
          await fs.promises.writeFile(outputFile, JSON.stringify(transformedData, null, 2));
          
          allTransformedData = allTransformedData.concat(transformedData);
          stats.recordsProcessed += Array.isArray(transformedData) ? transformedData.length : 1;
          
        } catch (error) {
          stats.errors++;
          logger.error(`Failed to transform ${file}:`, error);
        }
      }

      logger.info('Data transformation completed', stats);
      return { ...stats, transformedData: allTransformedData };
    } catch (error) {
      stats.errors++;
      logger.error('Data transformation failed:', error);
      throw error;
    }
  }

  async performTransformations(rawData) {
    if (!Array.isArray(rawData)) {
      rawData = [rawData];
    }

    return rawData.map(record => ({
      ...record,
      processed_timestamp: new Date().toISOString(),
      calibrated_value: this.calibrateValue(record),
      quality_score: this.calculateQualityScore(record)
    }));
  }

  calibrateValue(record) {
    if (typeof record.value !== 'number') return record.value;
    
    switch (record.reading_type || record.sensor_type) {
      case 'temperature':
        return Math.round((record.value * 1.02 - 0.5) * 100) / 100;
      case 'humidity':
        return Math.max(0, Math.min(100, Math.round((record.value * 0.98 + 1.5) * 100) / 100));
      default:
        return record.value;
    }
  }

  calculateQualityScore(record) {
    let score = 100;
    if (!record.timestamp) score -= 20;
    if (record.value === null || record.value === undefined) score -= 30;
    if (!record.sensor_id) score -= 15;
    if (record.battery_level < 20) score -= 10;
    return Math.max(0, score);
  }

  async queryTransformedData(filters = {}) {
    if (this.duckDBService) {
      return await this.duckDBService.queryData(filters);
    } else {
      // Fallback to file-based querying
      const files = await fs.promises.readdir(this.transformedDataPath);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      
      let allData = [];
      for (const file of jsonFiles) {
        try {
          const filePath = path.join(this.transformedDataPath, file);
          const data = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
          allData = allData.concat(Array.isArray(data) ? data : [data]);
        } catch (error) {
          logger.warn(`Failed to read ${file}:`, error.message);
        }
      }

      // Apply basic filtering
      return allData.filter(record => {
        if (filters.sensor_id && record.sensor_id !== filters.sensor_id) return false;
        if (filters.reading_type && record.reading_type !== filters.reading_type) return false;
        return true;
      });
    }
  }

  async getTransformationStats() {
    if (this.duckDBService) {
      return await this.duckDBService.getStats();
    } else {
      const data = await this.queryTransformedData();
      return {
        total: data.length,
        byType: data.reduce((acc, record) => {
          const type = record.reading_type;
          acc[type] = (acc[type] || 0) + 1;
          return acc;
        }, {}),
        source: 'file-based'
      };
    }
  }

  async checkBuildTools() {
    return await buildToolsChecker.getRecommendations();
  }
}

module.exports = TransformationService;
