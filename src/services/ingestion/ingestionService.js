const logger = require('../../utils/logger');
const FileUtils = require('../../utils/fileUtils');
const path = require('path');
const fs = require('fs');
const parquet = require('@dsnp/parquetjs');

class IngestionService {
  constructor(config) {
    this.config = config;
    this.initStorage();
  }

  initStorage() {
    try {
      logger.info('Ingestion storage initialization - using file-based approach temporarily');
      
      const dataDir = path.join(process.cwd(), 'data');
      const rawDir = path.join(dataDir, 'raw');
      
      [dataDir, rawDir].forEach(dir => {
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }
      });
      
      this.rawDataPath = rawDir;
      
      logger.info('Ingestion storage initialized successfully');
    } catch (error) {
      logger.error('Ingestion storage initialization failed:', error);
      throw error;
    }
  }

  async ingestData(options = {}) {
    logger.info('Starting data ingestion process', options);

    const stats = {
      recordsProcessed: 0,
      recordsSkipped: 0,
      errors: 0,
      filesProcessed: 0
    };

    try {
      // If options contains startDate/endDate, process files from raw directory
      if (options.startDate || options.endDate || !options.data) {
        const filesToProcess = await this.getFilesToProcess(options.startDate, options.endDate);
        
        logger.info(`Found ${filesToProcess.length} files to process`);
        
        for (const filePath of filesToProcess) {
          try {
            let fileStats;
            
            if (filePath.endsWith('.parquet')) {
              fileStats = await this.ingestParquetFile(filePath);
            } else if (filePath.endsWith('.json')) {
              fileStats = await this.ingestJsonFile(filePath);
            } else {
              logger.warn(`Unsupported file type: ${filePath}`);
              continue;
            }
            
            stats.recordsProcessed += fileStats.recordsProcessed;
            stats.recordsSkipped += fileStats.recordsSkipped;
            stats.errors += fileStats.errors;
            stats.filesProcessed++;
            
          } catch (error) {
            stats.errors++;
            logger.error(`Failed to process file ${filePath}:`, error);
          }
        }
      } else {
        // Legacy behavior: process data array directly
        const data = options.data || options;
        
        if (!Array.isArray(data)) {
          data = [data];
        }

        for (const record of data) {
          try {
            if (this.validateRecord(record)) {
              // Save to raw data file
              const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
              const filename = `ingested_${timestamp}.json`;
              const filePath = path.join(this.rawDataPath, filename);
              
              await fs.promises.writeFile(filePath, JSON.stringify([record], null, 2));
              stats.recordsProcessed++;
            } else {
              stats.recordsSkipped++;
            }
          } catch (error) {
            stats.errors++;
            logger.error('Failed to ingest record:', error);
          }
        }
      }

      logger.info('Data ingestion completed', stats);
      return stats;
    } catch (error) {
      logger.error('Data ingestion failed:', error);
      throw error;
    }
  }

  validateRecord(record) {
    // Basic validation
    if (!record.timestamp || !record.sensor_id || record.value === undefined) {
      return false;
    }
    
    // Check timestamp format
    if (isNaN(Date.parse(record.timestamp))) {
      return false;
    }
    
    return true;
  }

  async generateSampleData(count = 200) {
    logger.info(`Generating ${count} sample agricultural records`);
    
    const sensorTypes = [
      { type: 'temperature', unit: '°C', min: 10, max: 40 },
      { type: 'humidity', unit: '%', min: 30, max: 90 },
      { type: 'soil_moisture', unit: '%', min: 20, max: 80 },
      { type: 'ph_level', unit: 'pH', min: 5.5, max: 8.5 },
      { type: 'nitrogen', unit: 'ppm', min: 10, max: 100 },
      { type: 'phosphorus', unit: 'ppm', min: 5, max: 50 },
      { type: 'potassium', unit: 'ppm', min: 15, max: 120 }
    ];

    const locations = [
      { field: 'Field-A', lat: 28.6139, lon: 77.2090 },
      { field: 'Field-B', lat: 28.6129, lon: 77.2080 },
      { field: 'Field-C', lat: 28.6149, lon: 77.2100 }
    ];

    const records = [];
    const now = new Date();

    for (let i = 0; i < count; i++) {
      const sensorType = sensorTypes[Math.floor(Math.random() * sensorTypes.length)];
      const location = locations[Math.floor(Math.random() * locations.length)];
      
      // Generate timestamp within last 24 hours
      const timestamp = new Date(now.getTime() - Math.random() * 24 * 60 * 60 * 1000);
      
      const record = {
        sensor_id: `${location.field}-${sensorType.type}-${String(Math.floor(Math.random() * 10) + 1).padStart(2, '0')}`,
        timestamp: timestamp.toISOString(),
        reading_type: sensorType.type,
        value: parseFloat((Math.random() * (sensorType.max - sensorType.min) + sensorType.min).toFixed(2)),
        unit: sensorType.unit,
        location: {
          field: location.field,
          latitude: location.lat + (Math.random() - 0.5) * 0.01,
          longitude: location.lon + (Math.random() - 0.5) * 0.01
        },
        battery_level: Math.floor(Math.random() * 100),
        signal_strength: Math.floor(Math.random() * 100),
        data_quality: Math.random() > 0.1 ? 'good' : 'questionable'
      };

      records.push(record);
    }

    // Save sample data
    const filename = `sample_data_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    const filePath = path.join(this.rawDataPath, filename);
    await fs.promises.writeFile(filePath, JSON.stringify(records, null, 2));

    logger.info(`Generated ${count} sample records saved to ${filename}`);
    return records;
  }

  async validateSchema(schema) {
    const requiredColumns = ['sensor_id', 'timestamp', 'reading_type', 'value'];
    const schemaColumns = schema.map(col => col.column_name);
    
    for (const required of requiredColumns) {
      if (!schemaColumns.includes(required)) {
        throw new Error(`Missing required column: ${required}`);
      }
    }
    
    return true;
  }

  async getFilesToProcess(startDate, endDate) {
    try {
      const files = await fs.promises.readdir(this.rawDataPath);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      const parquetFiles = files.filter(f => f.endsWith('.parquet'));
      const allFiles = [...jsonFiles, ...parquetFiles];
      
      if (!startDate && !endDate) {
        return allFiles.map(f => path.join(this.rawDataPath, f));
      }
      
      const start = startDate ? new Date(startDate) : new Date(0);
      const end = endDate ? new Date(endDate) : new Date();
      
      const filteredFiles = [];
      
      for (const file of allFiles) {
        try {
          const filePath = path.join(this.rawDataPath, file);
          const stats = await fs.promises.stat(filePath);
          
          if (stats.mtime >= start && stats.mtime <= end) {
            filteredFiles.push(filePath);
          }
        } catch (error) {
          logger.warn(`Could not check file date for ${file}:`, error);
        }
      }
      
      return filteredFiles;
    } catch (error) {
      logger.error('Failed to get files to process:', error);
      return [];
    }
  }

  async ingestParquetFile(filePath) {
    logger.info(`Starting Parquet file ingestion: ${filePath}`);

    const stats = {
      recordsProcessed: 0,
      recordsSkipped: 0,
      errors: 0
    };

    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      
      let record = null;
      while (record = await cursor.next()) {
        try {
          // Convert Parquet record to plain JavaScript object and handle BigInt
          const plainRecord = {};
          for (const [key, value] of Object.entries(record)) {
            if (typeof value === 'bigint') {
              plainRecord[key] = Number(value);
            } else if (value && typeof value === 'object' && value.constructor === Object) {
              // Handle nested objects
              const nestedObj = {};
              for (const [nestedKey, nestedValue] of Object.entries(value)) {
                if (typeof nestedValue === 'bigint') {
                  nestedObj[nestedKey] = Number(nestedValue);
                } else {
                  nestedObj[nestedKey] = nestedValue;
                }
              }
              plainRecord[key] = nestedObj;
            } else {
              plainRecord[key] = value;
            }
          }

          if (this.validateRecord(plainRecord)) {
            // Save to raw data file (convert back to JSON for processing pipeline)
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const filename = `ingested_from_parquet_${timestamp}_${stats.recordsProcessed}.json`;
            const outputPath = path.join(this.rawDataPath, filename);
            
            await fs.promises.writeFile(outputPath, JSON.stringify([plainRecord], null, 2));
            stats.recordsProcessed++;
          } else {
            stats.recordsSkipped++;
          }
        } catch (error) {
          stats.errors++;
          logger.error('Failed to process Parquet record:', error);
        }
      }

      await reader.close();

      logger.info(`Parquet file ingestion completed: ${filePath}`, stats);
      return stats;
    } catch (error) {
      logger.error(`Parquet file ingestion failed: ${filePath}`, error);
      throw error;
    }
  }

  async ingestJsonFile(filePath) {
    logger.info(`Starting JSON file ingestion: ${filePath}`);

    const stats = {
      recordsProcessed: 0,
      recordsSkipped: 0,
      errors: 0
    };

    try {
      const fileContent = await fs.promises.readFile(filePath, 'utf8');
      const data = JSON.parse(fileContent);
      
      const records = Array.isArray(data) ? data : [data];
      
      for (const record of records) {
        try {
          if (this.validateRecord(record)) {
            // Process the record (it's already ingested, just validate)
            stats.recordsProcessed++;
          } else {
            stats.recordsSkipped++;
          }
        } catch (error) {
          stats.errors++;
          logger.error('Failed to process JSON record:', error);
        }
      }

      logger.info(`JSON file ingestion completed: ${filePath}`, stats);
      return stats;
    } catch (error) {
      logger.error(`JSON file ingestion failed: ${filePath}`, error);
      throw error;
    }
  }
}

module.exports = IngestionService;
