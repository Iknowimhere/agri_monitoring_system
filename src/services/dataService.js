// const duckdb = require('duckdb'); // TODO: Will use DuckDB when C++ build tools are available
const logger = require('../utils/logger');
const DateUtils = require('../utils/dateUtils');
const path = require('path');
const fs = require('fs');
const parquet = require('@dsnp/parquetjs');

class DataService {
  constructor() {
    this.initStorage();
  }

  initStorage() {
    try {
      logger.info('Data service initialization - using file-based approach temporarily');
      
      const dataDir = path.join(process.cwd(), 'data');
      const processedDir = path.join(dataDir, 'processed');
      
      [dataDir, processedDir].forEach(dir => {
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }
      });
      
      this.processedDataPath = processedDir;
      
      logger.info('Data service initialized successfully');
    } catch (error) {
      logger.error('Data service initialization failed:', error);
      throw error;
    }
  }

  async queryData(filters) {
    logger.info('Querying processed data', filters);

    try {
      // Load all processed data files
      const allData = await this.loadProcessedData();
      
      // Apply filters
      let filteredData = allData;
      
      if (filters.sensor_id) {
        filteredData = filteredData.filter(record => record.sensor_id === filters.sensor_id);
      }

      if (filters.reading_type) {
        filteredData = filteredData.filter(record => record.reading_type === filters.reading_type);
      }

      if (filters.startDate) {
        const startDate = new Date(filters.startDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) >= startDate);
      }

      if (filters.endDate) {
        const endDate = new Date(filters.endDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) <= endDate);
      }

      if (filters.anomalous !== undefined) {
        filteredData = filteredData.filter(record => record.anomalous_reading === filters.anomalous);
      }

      if (filters.minValue !== undefined) {
        filteredData = filteredData.filter(record => record.value >= filters.minValue);
      }

      if (filters.maxValue !== undefined) {
        filteredData = filteredData.filter(record => record.value <= filters.maxValue);
      }

      // Apply sorting
      const sortBy = filters.sortBy || 'timestamp';
      const sortOrder = filters.sortOrder || 'DESC';
      
      filteredData.sort((a, b) => {
        const aVal = a[sortBy];
        const bVal = b[sortBy];
        
        if (sortOrder === 'ASC') {
          return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
        } else {
          return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
        }
      });

      // Apply pagination
      const page = filters.page || 1;
      const limit = filters.limit || 100;
      const offset = (page - 1) * limit;
      const paginatedData = filteredData.slice(offset, offset + limit);

      return {
        data: paginatedData,
        page,
        limit,
        total: filteredData.length
      };
    } catch (error) {
      logger.error('Failed to query data:', error);
      throw error;
    }
  }

  async loadProcessedData() {
    const allData = [];
    
    if (!fs.existsSync(this.processedDataPath)) {
      return allData;
    }
    
    const files = await fs.promises.readdir(this.processedDataPath);
    const jsonFiles = files.filter(f => f.endsWith('.json'));
    const parquetFiles = files.filter(f => f.endsWith('.parquet'));
    
    // Load JSON files
    for (const file of jsonFiles) {
      try {
        const filePath = path.join(this.processedDataPath, file);
        const data = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
        const records = Array.isArray(data) ? data : [data];
        allData.push(...records);
      } catch (error) {
        logger.error(`Failed to load JSON data file ${file}:`, error);
      }
    }
    
    // Load Parquet files
    for (const file of parquetFiles) {
      try {
        const filePath = path.join(this.processedDataPath, file);
        const records = await this.readParquetFile(filePath);
        allData.push(...records);
      } catch (error) {
        logger.error(`Failed to load Parquet data file ${file}:`, error);
      }
    }
    
    return allData;
  }

  async getSensors() {
    logger.info('Getting sensors list');

    try {
      const allData = await this.loadProcessedData();
      
      // Group data by sensor_id to get sensor summary
      const sensorMap = new Map();
      
      for (const record of allData) {
        if (!sensorMap.has(record.sensor_id)) {
          sensorMap.set(record.sensor_id, {
            sensor_id: record.sensor_id,
            last_reading: record.timestamp,
            reading_types: new Set(),
            total_readings: 0
          });
        }
        
        const sensor = sensorMap.get(record.sensor_id);
        sensor.reading_types.add(record.reading_type);
        sensor.total_readings++;
        
        // Update last reading if this is more recent
        if (new Date(record.timestamp) > new Date(sensor.last_reading)) {
          sensor.last_reading = record.timestamp;
        }
      }
      
      // Convert to array and format
      const sensors = Array.from(sensorMap.values()).map(sensor => ({
        ...sensor,
        reading_types: Array.from(sensor.reading_types)
      }));
      
      return sensors;
    } catch (error) {
      logger.error('Failed to get sensors:', error);
      throw error;
    }
  }

  async getSensorSummary(sensorId, options = {}) {
    logger.info('Getting sensor summary', { sensorId, ...options });

    try {
      const allData = await this.loadProcessedData();
      
      // Filter by sensor_id
      let sensorData = allData.filter(record => record.sensor_id === sensorId);
      
      // Apply date filters if provided
      if (options.startDate) {
        const startDate = new Date(options.startDate);
        sensorData = sensorData.filter(record => new Date(record.timestamp) >= startDate);
      }
      
      if (options.endDate) {
        const endDate = new Date(options.endDate);
        sensorData = sensorData.filter(record => new Date(record.timestamp) <= endDate);
      }
      
      // Group by reading_type and calculate statistics
      const summaryMap = new Map();
      
      for (const record of sensorData) {
        if (!summaryMap.has(record.reading_type)) {
          summaryMap.set(record.reading_type, {
            reading_type: record.reading_type,
            total_readings: 0,
            values: [],
            anomalous_count: 0
          });
        }
        
        const summary = summaryMap.get(record.reading_type);
        summary.total_readings++;
        summary.values.push(record.value);
        
        if (record.anomalous_reading) {
          summary.anomalous_count++;
        }
      }
      
      // Calculate statistics for each reading type
      const result = Array.from(summaryMap.values()).map(summary => {
        const values = summary.values.filter(v => typeof v === 'number');
        
        return {
          reading_type: summary.reading_type,
          total_readings: summary.total_readings,
          avg_value: values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0,
          min_value: values.length > 0 ? Math.min(...values) : 0,
          max_value: values.length > 0 ? Math.max(...values) : 0,
          std_deviation: this.calculateStdDev(values),
          anomalous_count: summary.anomalous_count
        };
      });
      
      return result;
    } catch (error) {
      logger.error('Failed to get sensor summary:', error);
      throw error;
    }
  }

  calculateStdDev(values) {
    if (values.length === 0) return 0;
    
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const squaredDiffs = values.map(value => Math.pow(value - mean, 2));
    const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / values.length;
    
    return Math.sqrt(avgSquaredDiff);
  }

  async getDailyAggregations(filters) {
    logger.info('Getting daily aggregations', filters);

    try {
      const allData = await this.loadProcessedData();
      
      // Apply filters
      let filteredData = allData;
      
      if (filters.reading_type) {
        filteredData = filteredData.filter(record => record.reading_type === filters.reading_type);
      }
      
      if (filters.startDate) {
        const startDate = new Date(filters.startDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) >= startDate);
      }
      
      if (filters.endDate) {
        const endDate = new Date(filters.endDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) <= endDate);
      }
      
      // Group by date and reading_type
      const aggregationMap = new Map();
      
      for (const record of filteredData) {
        const date = new Date(record.timestamp).toISOString().split('T')[0];
        const key = `${date}_${record.reading_type}`;
        
        if (!aggregationMap.has(key)) {
          aggregationMap.set(key, {
            date,
            reading_type: record.reading_type,
            values: [],
            reading_count: 0
          });
        }
        
        const agg = aggregationMap.get(key);
        if (typeof record.value === 'number') {
          agg.values.push(record.value);
        }
        agg.reading_count++;
      }
      
      // Calculate aggregations
      const result = Array.from(aggregationMap.values()).map(agg => ({
        date: agg.date,
        reading_type: agg.reading_type,
        avg_value: agg.values.length > 0 ? agg.values.reduce((a, b) => a + b, 0) / agg.values.length : 0,
        min_value: agg.values.length > 0 ? Math.min(...agg.values) : 0,
        max_value: agg.values.length > 0 ? Math.max(...agg.values) : 0,
        reading_count: agg.reading_count
      })).sort((a, b) => {
        if (a.date !== b.date) return a.date.localeCompare(b.date);
        return a.reading_type.localeCompare(b.reading_type);
      });
      
      return result;
    } catch (error) {
      logger.error('Failed to get daily aggregations:', error);
      throw error;
    }
  }

  async getAnomalies(filters) {
    logger.info('Getting anomalies', filters);

    try {
      const anomalyFilters = { ...filters, anomalous: true };
      return await this.queryData(anomalyFilters);
    } catch (error) {
      logger.error('Failed to get anomalies:', error);
      throw error;
    }
  }

  async exportData(format, filters) {
    logger.info('Exporting data', { format, filters });

    try {
      const result = await this.queryData({ ...filters, limit: 10000 }); // Large limit for export
      const data = result.data;

      switch (format) {
        case 'csv':
          return this.convertToCSV(data);
        case 'json':
          return JSON.stringify(data, null, 2);
        case 'parquet':
          return await this.exportToParquet(data, filters);
        default:
          throw new Error(`Unsupported export format: ${format}`);
      }
    } catch (error) {
      logger.error('Failed to export data:', error);
      throw error;
    }
  }

  convertToCSV(data) {
    if (!data || data.length === 0) {
      return '';
    }

    const headers = Object.keys(data[0]);
    const csvLines = [headers.join(',')];

    for (const row of data) {
      const line = headers.map(header => {
        const value = row[header];
        return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : value;
      }).join(',');
      csvLines.push(line);
    }

    return csvLines.join('\n');
  }

  async getQueryCount(filters) {
    // Implementation to get total count for pagination
    try {
      const allData = await this.loadProcessedData();
      
      // Apply the same filters as queryData to get accurate count
      let filteredData = allData;
      
      if (filters.sensor_id) {
        filteredData = filteredData.filter(record => record.sensor_id === filters.sensor_id);
      }

      if (filters.reading_type) {
        filteredData = filteredData.filter(record => record.reading_type === filters.reading_type);
      }

      if (filters.startDate) {
        const startDate = new Date(filters.startDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) >= startDate);
      }

      if (filters.endDate) {
        const endDate = new Date(filters.endDate);
        filteredData = filteredData.filter(record => new Date(record.timestamp) <= endDate);
      }

      if (filters.anomalous !== undefined) {
        filteredData = filteredData.filter(record => record.anomalous_reading === filters.anomalous);
      }

      if (filters.minValue !== undefined) {
        filteredData = filteredData.filter(record => record.value >= filters.minValue);
      }

      if (filters.maxValue !== undefined) {
        filteredData = filteredData.filter(record => record.value <= filters.maxValue);
      }
      
      return filteredData.length;
    } catch (error) {
      logger.error('Failed to get query count:', error);
      return 0;
    }
  }

  async readParquetFile(filePath) {
    try {
      logger.info(`Reading Parquet file: ${filePath}`);
      
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      const records = [];
      
      let record = null;
      while (record = await cursor.next()) {
        // Convert Parquet record to plain JavaScript object and handle BigInt
        const plainRecord = {};
        for (const [key, value] of Object.entries(record)) {
          if (typeof value === 'bigint') {
            // Convert BigInt to Number (safe for most sensor data)
            plainRecord[key] = Number(value);
          } else if (value && typeof value === 'object' && value.constructor === Object) {
            // Handle nested objects (like location)
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
        records.push(plainRecord);
      }
      
      await reader.close();
      logger.info(`Successfully read ${records.length} records from Parquet file: ${filePath}`);
      
      return records;
    } catch (error) {
      logger.error(`Failed to read Parquet file ${filePath}:`, error);
      throw error;
    }
  }

  async exportToParquet(data, filters) {
    try {
      if (!data || data.length === 0) {
        throw new Error('No data to export');
      }

      // Create filename with timestamp and filters
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const filterSuffix = filters.reading_type ? `_${filters.reading_type}` : '';
      const filename = `export${filterSuffix}_${timestamp}.parquet`;
      const filePath = path.join(this.processedDataPath, filename);

      // Define Parquet schema based on data structure
      const sampleRecord = data[0];
      const schema = new parquet.ParquetSchema({
        sensor_id: { type: 'UTF8' },
        timestamp: { type: 'TIMESTAMP_MILLIS' },
        reading_type: { type: 'UTF8' },
        value: { type: 'DOUBLE' },
        unit: { type: 'UTF8', optional: true },
        location: {
          optional: true,
          fields: {
            field: { type: 'UTF8', optional: true },
            latitude: { type: 'DOUBLE', optional: true },
            longitude: { type: 'DOUBLE', optional: true }
          }
        },
        battery_level: { type: 'INT32', optional: true },
        signal_strength: { type: 'INT32', optional: true },
        data_quality: { type: 'UTF8', optional: true },
        processed_timestamp: { type: 'TIMESTAMP_MILLIS', optional: true },
        calibrated_value: { type: 'DOUBLE', optional: true },
        quality_score: { type: 'DOUBLE', optional: true },
        anomalous_reading: { type: 'BOOLEAN', optional: true }
      });

      // Create Parquet writer
      const writer = await parquet.ParquetWriter.openFile(schema, filePath);

      // Write data records
      for (const record of data) {
        const parquetRecord = {
          sensor_id: record.sensor_id || '',
          timestamp: new Date(record.timestamp),
          reading_type: record.reading_type || '',
          value: parseFloat(record.value) || 0,
          unit: record.unit || null,
          location: record.location ? {
            field: record.location.field || null,
            latitude: record.location.latitude || null,
            longitude: record.location.longitude || null
          } : null,
          battery_level: record.battery_level ? parseInt(record.battery_level) : null,
          signal_strength: record.signal_strength ? parseInt(record.signal_strength) : null,
          data_quality: record.data_quality || null,
          processed_timestamp: record.processed_timestamp ? new Date(record.processed_timestamp) : null,
          calibrated_value: record.calibrated_value ? parseFloat(record.calibrated_value) : null,
          quality_score: record.quality_score ? parseFloat(record.quality_score) : null,
          anomalous_reading: record.anomalous_reading || false
        };

        await writer.appendRow(parquetRecord);
      }

      await writer.close();
      
      logger.info(`Successfully exported ${data.length} records to Parquet file: ${filePath}`);
      
      return {
        format: 'parquet',
        filename,
        path: filePath,
        records: data.length,
        message: `Data exported successfully to ${filename}`
      };
    } catch (error) {
      logger.error('Failed to export to Parquet:', error);
      throw error;
    }
  }
}

module.exports = new DataService();
