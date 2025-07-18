const logger = require('../utils/logger');
const DuckDBService = require('./duckDBService');
const duckDBSingleton = require('./duckDBSingleton');
const DateUtils = require('../utils/dateUtils');
const config = require('../config/config');

class DataService {
  constructor() {
    this.duckDBService = null;
    this.initialized = false;
  }

  async initializeDatabase() {
    if (this.initialized) return;
    
    try {
      logger.info('DataService: Initializing DuckDB connection...');
      
      // Check if we're in test mode and use global mock
      const isTestMode = process.env.NODE_ENV === 'test' || global.testUtils;
      
      if (isTestMode && global.mockDuckDBService) {
        this.duckDBService = global.mockDuckDBService;
        logger.info('DataService: Using mocked DuckDB service for testing');
      } else {
        this.duckDBService = await duckDBSingleton.getInstance();
      }
      
      this.initialized = true;
      logger.info('DataService: DuckDB initialized successfully', { 
        mode: this.duckDBService.dbType || 'fallback',
        success: this.duckDBService.isAvailable 
      });
    } catch (error) {
      logger.error('DataService: Failed to initialize DuckDB:', error);
      throw error;
    }
  }

  async ensureInitialized() {
    if (!this.initialized) {
      await this.initializeDatabase();
    }
  }

  async queryData(filters) {
    await this.ensureInitialized();
    logger.info('Querying data with DuckDB', filters);

    try {
      // Convert filters to DuckDB format
      const duckDBFilters = {
        sensor_id: filters.sensor_id,
        reading_type: filters.reading_type,
        startDate: filters.startDate,
        endDate: filters.endDate,
        field: filters.field,
        limit: filters.limit
      };

      // Query data from DuckDB
      const result = await this.duckDBService.queryData(duckDBFilters);
      
      // Apply additional filtering that DuckDB service doesn't handle
      let filteredData = result.data;

      if (filters.anomalous !== undefined) {
        filteredData = filteredData.filter(record => 
          record.anomalous_reading === filters.anomalous
        );
      }

      if (filters.minValue !== undefined) {
        filteredData = filteredData.filter(record => 
          record.value >= filters.minValue
        );
      }

      if (filters.maxValue !== undefined) {
        filteredData = filteredData.filter(record => 
          record.value <= filters.maxValue
        );
      }

      // Apply custom sorting if different from timestamp
      if (filters.sortBy && filters.sortBy !== 'timestamp') {
        const sortBy = filters.sortBy;
        const sortOrder = (filters.sortOrder || 'DESC').toUpperCase();
        
        filteredData.sort((a, b) => {
          const aVal = a[sortBy];
          const bVal = b[sortBy];
          
          if (sortOrder === 'ASC') {
            return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
          } else {
            return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
          }
        });
      }

      // Apply pagination (DuckDB handles limit, but we need page-based pagination)
      const page = filters.page || 1;
      const limit = filters.limit || 100;
      const offset = (page - 1) * limit;
      
      // If we didn't get all data due to DuckDB limit, we need to re-query
      if (!filters.limit || filters.limit > limit) {
        const paginatedData = filteredData.slice(offset, offset + limit);
        
        return {
          success: true,
          data: paginatedData,
          page,
          limit,
          total: filteredData.length,
          source: result.source
        };
      }

      return {
        success: true,
        data: filteredData,
        page,
        limit,
        total: filteredData.length,
        source: result.source
      };

    } catch (error) {
      logger.error('Failed to query data:', error);
      throw error;
    }
  }

  async getSensors() {
    await this.ensureInitialized();
    logger.info('Getting unique sensors with DuckDB');

    try {
      const result = await this.duckDBService.queryData({});
      const data = result.data;
      
      // Extract unique sensors and their information
      const sensorMap = new Map();
      
      data.forEach(record => {
        if (!sensorMap.has(record.sensor_id)) {
          sensorMap.set(record.sensor_id, {
            sensor_id: record.sensor_id,
            reading_types: new Set(),
            locations: new Set(),
            latest_reading: record.timestamp,
            total_readings: 0
          });
        }
        
        const sensor = sensorMap.get(record.sensor_id);
        sensor.reading_types.add(record.reading_type);
        
        if (record.field) {
          sensor.locations.add(record.field);
        }
        
        if (new Date(record.timestamp) > new Date(sensor.latest_reading)) {
          sensor.latest_reading = record.timestamp;
        }
        
        sensor.total_readings++;
      });

      // Convert Sets to Arrays for JSON serialization
      const sensors = Array.from(sensorMap.values()).map(sensor => ({
        ...sensor,
        reading_types: Array.from(sensor.reading_types),
        locations: Array.from(sensor.locations)
      }));

      logger.info('Retrieved sensors', { count: sensors.length });
      return sensors;

    } catch (error) {
      logger.error('Failed to get sensors:', error);
      throw error;
    }
  }

  async getSensorSummary(sensorId, options = {}) {
    await this.ensureInitialized();
    logger.info('Getting sensor summary with DuckDB', { sensorId, options });

    try {
      const filters = {
        sensor_id: sensorId,
        startDate: options.startDate,
        endDate: options.endDate
      };

      const result = await this.duckDBService.queryData(filters);
      const data = result.data;

      if (data.length === 0) {
        return {
          sensor_id: sensorId,
          total_readings: 0,
          reading_types: [],
          date_range: null,
          latest_reading: null,
          statistics: {}
        };
      }

      // Calculate summary statistics
      const readingTypes = [...new Set(data.map(r => r.reading_type))];
      const statistics = {};

      readingTypes.forEach(type => {
        const typeData = data.filter(r => r.reading_type === type);
        const values = typeData.map(r => r.value).filter(v => v !== null && v !== undefined);
        
        if (values.length > 0) {
          statistics[type] = {
            count: values.length,
            min: Math.min(...values),
            max: Math.max(...values),
            avg: values.reduce((a, b) => a + b, 0) / values.length,
            latest_value: typeData[0].value,
            latest_timestamp: typeData[0].timestamp
          };
        }
      });

      const timestamps = data.map(r => new Date(r.timestamp));
      const summary = {
        sensor_id: sensorId,
        total_readings: data.length,
        reading_types: readingTypes,
        date_range: {
          start: new Date(Math.min(...timestamps)).toISOString(),
          end: new Date(Math.max(...timestamps)).toISOString()
        },
        latest_reading: data[0].timestamp,
        statistics,
        source: result.source
      };

      return summary;

    } catch (error) {
      logger.error('Failed to get sensor summary:', error);
      throw error;
    }
  }

  async getDailyAggregations(filters) {
    await this.ensureInitialized();
    logger.info('Getting daily aggregations with DuckDB', filters);

    try {
      // Use DuckDB for efficient aggregation
      const result = await this.duckDBService.queryData({
        sensor_id: filters.sensor_id,
        reading_type: filters.reading_type,
        startDate: filters.startDate,
        endDate: filters.endDate
      });

      const data = result.data;

      // Group by date
      const dailyData = {};
      
      data.forEach(record => {
        const date = new Date(record.timestamp).toISOString().split('T')[0];
        
        if (!dailyData[date]) {
          dailyData[date] = {
            date,
            readings: [],
            count: 0
          };
        }
        
        dailyData[date].readings.push(record.value);
        dailyData[date].count++;
      });

      // Calculate aggregations
      const aggregations = Object.values(dailyData).map(day => {
        const values = day.readings.filter(v => v !== null && v !== undefined);
        
        return {
          date: day.date,
          count: day.count,
          min: values.length > 0 ? Math.min(...values) : null,
          max: values.length > 0 ? Math.max(...values) : null,
          avg: values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : null,
          sum: values.length > 0 ? values.reduce((a, b) => a + b, 0) : null
        };
      });

      // Sort by date
      aggregations.sort((a, b) => new Date(a.date) - new Date(b.date));

      return {
        aggregations,
        total_days: aggregations.length,
        source: result.source
      };

    } catch (error) {
      logger.error('Failed to get daily aggregations:', error);
      throw error;
    }
  }

  async getAnomalies(filters) {
    await this.ensureInitialized();
    logger.info('Getting anomalies with DuckDB', filters);

    try {
      const result = await this.duckDBService.queryData(filters);
      const anomalies = result.data.filter(record => record.anomalous_reading === true);

      return {
        data: anomalies,
        total: anomalies.length,
        source: result.source
      };

    } catch (error) {
      logger.error('Failed to get anomalies:', error);
      throw error;
    }
  }

  async exportData(format, filters) {
    await this.ensureInitialized();
    logger.info('Exporting data with DuckDB', { format, filters });

    try {
      const result = await this.duckDBService.queryData(filters);
      const data = result.data;

      // Convert BigInt values to regular numbers to avoid JSON serialization issues
      const sanitizedData = data.map(record => {
        const sanitized = {};
        for (const [key, value] of Object.entries(record)) {
          if (typeof value === 'bigint') {
            sanitized[key] = Number(value);
          } else {
            sanitized[key] = value;
          }
        }
        return sanitized;
      });

      switch (format.toLowerCase()) {
        case 'csv':
          return this.convertToCSV(sanitizedData);
        case 'json':
          return JSON.stringify(sanitizedData, null, 2);
        default:
          throw new Error(`Unsupported export format: ${format}`);
      }

    } catch (error) {
      logger.error('Failed to export data:', error);
      throw error;
    }
  }

  convertToCSV(data) {
    if (data.length === 0) return '';
    
    const headers = Object.keys(data[0]);
    const csvRows = [headers.join(',')];
    
    data.forEach(row => {
      const values = headers.map(header => {
        const value = row[header];
        return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : value;
      });
      csvRows.push(values.join(','));
    });
    
    return csvRows.join('\n');
  }

  async getQueryCount(filters) {
    await this.ensureInitialized();
    
    try {
      const stats = await this.duckDBService.getStats();
      
      // Apply filter-based counting if needed
      if (Object.keys(filters).length > 0) {
        const result = await this.duckDBService.queryData(filters);
        return {
          total: result.data.length,
          source: result.source
        };
      }
      
      return {
        total: stats.total,
        source: stats.source
      };

    } catch (error) {
      logger.error('Failed to get query count:', error);
      throw error;
    }
  }

  async insertData(data) {
    await this.ensureInitialized();
    
    try {
      return await this.duckDBService.insertData(data);
    } catch (error) {
      logger.error('Failed to insert data:', error);
      throw error;
    }
  }

  async updateData(id, updates) {
    await this.ensureInitialized();
    
    try {
      return await this.duckDBService.updateData(id, updates);
    } catch (error) {
      logger.error('Failed to update data:', error);
      throw error;
    }
  }

  async deleteData(id) {
    await this.ensureInitialized();
    
    try {
      return await this.duckDBService.deleteData(id);
    } catch (error) {
      logger.error('Failed to delete data:', error);
      throw error;
    }
  }

  async getStats() {
    await this.ensureInitialized();
    
    try {
      return await this.duckDBService.getStats();
    } catch (error) {
      logger.error('Failed to get stats:', error);
      throw error;
    }
  }

  async readParquetFile(filePath, options = {}) {
    await this.ensureInitialized();
    logger.info('Reading Parquet file with DuckDB', { filePath, options });
    
    try {
      // Use DuckDB to read Parquet files directly
      const result = await this.duckDBService.readParquet(filePath, options);
      
      logger.info('Successfully read Parquet file', { 
        filePath, 
        recordCount: result.data ? result.data.length : 0 
      });
      
      return result;
    } catch (error) {
      logger.error('Failed to read Parquet file:', error);
      throw error;
    }
  }

  async close() {
    if (this.duckDBService) {
      await this.duckDBService.close();
    }
  }
}

// Export singleton instance
module.exports = new DataService();
