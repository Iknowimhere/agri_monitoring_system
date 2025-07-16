const fs = require('fs').promises;
const path = require('path');
const logger = require('../../utils/logger');
const FileUtils = require('../../utils/fileUtils');
const DateUtils = require('../../utils/dateUtils');

class StorageService {
  constructor(config) {
    this.config = config;
    this.dataPath = path.join(config.paths.data, 'processed');
    this.dbPath = path.join(config.paths.data, 'database');
    this.ensureDirectories();
  }

  async ensureDirectories() {
    try {
      await fs.mkdir(this.dataPath, { recursive: true });
      await fs.mkdir(this.dbPath, { recursive: true });
    } catch (error) {
      logger.error('Failed to create storage directories:', error);
    }
  }

  // TODO: Replace with DuckDB when C++ build tools are available
  async storeData(data = null) {
    logger.info('Starting data storage process');
    
    const statistics = {
      filesWritten: 0,
      recordsStored: 0,
      errors: 0
    };

    try {
      // If no data provided, read from transformation output
      if (!data) {
        const transformedDataPath = path.join(this.config.paths.data, 'transformed');
        const files = await fs.readdir(transformedDataPath);
        
        for (const file of files) {
          if (file.endsWith('.json')) {
            const filePath = path.join(transformedDataPath, file);
            const fileData = JSON.parse(await fs.readFile(filePath, 'utf8'));
            await this.storeRecords(fileData, file);
            statistics.filesWritten++;
            statistics.recordsStored += Array.isArray(fileData) ? fileData.length : 1;
          }
        }
      } else {
        await this.storeRecords(data, 'direct_data.json');
        statistics.filesWritten = 1;
        statistics.recordsStored = Array.isArray(data) ? data.length : 1;
      }

      // Create a master index file
      await this.createMasterIndex();

      logger.info('Data storage completed successfully', statistics);
      return statistics;
    } catch (error) {
      statistics.errors++;
      logger.error('Data storage failed:', error);
      throw error;
    }
  }

  async storeRecords(records, filename) {
    try {
      const outputPath = path.join(this.dataPath, filename);
      
      // For now, store as JSON files
      // TODO: Replace with DuckDB storage
      await fs.writeFile(outputPath, JSON.stringify(records, null, 2));
      
      // Also append to a daily aggregated file
      const date = DateUtils.nowIST().toISOString().split('T')[0];
      const dailyFile = path.join(this.dataPath, `daily_${date}.jsonl`);
      
      if (Array.isArray(records)) {
        const jsonLines = records.map(record => JSON.stringify(record)).join('\n') + '\n';
        await fs.appendFile(dailyFile, jsonLines);
      } else {
        await fs.appendFile(dailyFile, JSON.stringify(records) + '\n');
      }

      logger.debug(`Stored ${Array.isArray(records) ? records.length : 1} records to ${filename}`);
    } catch (error) {
      logger.error(`Failed to store records to ${filename}:`, error);
      throw error;
    }
  }

  async createMasterIndex() {
    try {
      const files = await fs.readdir(this.dataPath);
      const index = {
        lastUpdated: DateUtils.nowIST().toISOString(),
        files: [],
        totalRecords: 0
      };

      for (const file of files) {
        if (file.endsWith('.json')) {
          const filePath = path.join(this.dataPath, file);
          const stats = await fs.stat(filePath);
          const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
          const recordCount = Array.isArray(data) ? data.length : 1;
          
          index.files.push({
            filename: file,
            size: stats.size,
            created: stats.birthtime,
            modified: stats.mtime,
            recordCount
          });
          
          index.totalRecords += recordCount;
        }
      }

      const indexPath = path.join(this.dataPath, 'index.json');
      await fs.writeFile(indexPath, JSON.stringify(index, null, 2));
      
      logger.debug('Master index created successfully');
    } catch (error) {
      logger.error('Failed to create master index:', error);
    }
  }

  // Query interface (will be replaced with DuckDB queries)
  async queryData(query = {}) {
    logger.info('Querying stored data', query);
    
    try {
      const { 
        startDate, 
        endDate, 
        sensorType, 
        farmId, 
        limit = 1000,
        offset = 0 
      } = query;

      // For now, query JSON files
      // TODO: Replace with DuckDB SQL queries
      const files = await fs.readdir(this.dataPath);
      let results = [];

      for (const file of files) {
        if (file.endsWith('.json') && file !== 'index.json') {
          const filePath = path.join(this.dataPath, file);
          const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
          
          if (Array.isArray(data)) {
            results = results.concat(data);
          } else {
            results.push(data);
          }
        }
      }

      // Apply filters
      if (startDate || endDate || sensorType || farmId) {
        results = results.filter(record => {
          if (startDate && record.timestamp && new Date(record.timestamp) < new Date(startDate)) {
            return false;
          }
          if (endDate && record.timestamp && new Date(record.timestamp) > new Date(endDate)) {
            return false;
          }
          if (sensorType && record.sensorType !== sensorType) {
            return false;
          }
          if (farmId && record.farmId !== farmId) {
            return false;
          }
          return true;
        });
      }

      // Apply pagination
      const total = results.length;
      results = results.slice(offset, offset + limit);

      return {
        data: results,
        total,
        limit,
        offset,
        hasMore: offset + limit < total
      };
    } catch (error) {
      logger.error('Query failed:', error);
      throw error;
    }
  }

  // Initialize DuckDB when build tools are available
  async initializeDuckDB() {
    try {
      // This will be implemented when DuckDB is available
      logger.info('DuckDB initialization will be implemented when C++ build tools are configured');
      
      // Placeholder for DuckDB initialization:
      // const Database = require('duckdb').Database;
      // this.db = new Database(path.join(this.dbPath, 'agricultural_data.duckdb'));
      
      return { success: false, message: 'DuckDB requires C++ build tools' };
    } catch (error) {
      logger.error('DuckDB initialization failed:', error);
      return { success: false, error: error.message };
    }
  }

  async getStorageStats() {
    try {
      const indexPath = path.join(this.dataPath, 'index.json');
      
      if (await FileUtils.fileExists(indexPath)) {
        const index = JSON.parse(await fs.readFile(indexPath, 'utf8'));
        return index;
      }

      // Fallback: calculate stats manually
      const files = await fs.readdir(this.dataPath);
      let totalRecords = 0;
      let totalSize = 0;

      for (const file of files) {
        if (file.endsWith('.json')) {
          const filePath = path.join(this.dataPath, file);
          const stats = await fs.stat(filePath);
          totalSize += stats.size;
          
          const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
          totalRecords += Array.isArray(data) ? data.length : 1;
        }
      }

      return {
        totalRecords,
        totalSize,
        fileCount: files.filter(f => f.endsWith('.json')).length,
        lastUpdated: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Failed to get storage stats:', error);
      return { error: error.message };
    }
  }
}

module.exports = StorageService;
