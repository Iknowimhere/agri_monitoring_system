// const Database = require('duckdb'); // TODO: Will use DuckDB when C++ build tools are available
const logger = require('../../utils/logger');
const path = require('path');
const fs = require('fs');

class TransformationService {
  constructor(config) {
    this.config = config;
    this.initStorage();
  }

  initStorage() {
    try {
      logger.info('Transformation storage initialization - using file-based approach temporarily');
      
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
      
      logger.info('Transformation storage initialized successfully');
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
      errors: 0
    };

    try {
      const files = await fs.promises.readdir(this.rawDataPath);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      
      if (jsonFiles.length === 0) {
        logger.warn('No raw data files found for transformation');
        return stats;
      }

      for (const file of jsonFiles) {
        try {
          const filePath = path.join(this.rawDataPath, file);
          const rawData = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
          
          const transformedData = await this.performTransformations(rawData);
          
          const outputFile = path.join(this.transformedDataPath, `transformed_${file}`);
          await fs.promises.writeFile(outputFile, JSON.stringify(transformedData, null, 2));
          
          stats.recordsProcessed += Array.isArray(transformedData) ? transformedData.length : 1;
          
        } catch (error) {
          stats.errors++;
          logger.error(`Failed to transform ${file}:`, error);
        }
      }

      logger.info('Data transformation completed', stats);
      return stats;
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
}

module.exports = TransformationService;
