const logger = require('../utils/logger');
const DataService = require('../services/dataService');

class DataController {
  static async queryData(req, res, next) {
    try {
      const filters = { ...req.query };
      
      // Parse numeric query parameters
      if (filters.page) filters.page = parseInt(filters.page, 10);
      if (filters.limit) filters.limit = parseInt(filters.limit, 10);
      if (filters.minValue) filters.minValue = parseFloat(filters.minValue);
      if (filters.maxValue) filters.maxValue = parseFloat(filters.maxValue);
      if (filters.anomalous) filters.anomalous = filters.anomalous === 'true';
      
      // Apply defaults only if not provided
      if (!filters.page) filters.page = 1;
      if (!filters.limit) filters.limit = 100;
      if (!filters.sortBy) filters.sortBy = 'timestamp';
      if (!filters.sortOrder) filters.sortOrder = 'desc';
      
      const result = await DataService.queryData(filters);
      
      res.json({
        data: result.data,
        pagination: {
          page: result.page,
          limit: result.limit,
          total: result.total,
          pages: Math.ceil(result.total / result.limit)
        },
        filters: filters
      });
    } catch (error) {
      next(error);
    }
  }

  static async getSensors(req, res, next) {
    try {
      const sensors = await DataService.getSensors();
      
      res.json({
        sensors,
        total_sensors: sensors.length
      });
    } catch (error) {
      next(error);
    }
  }

  static async getSensorSummary(req, res, next) {
    try {
      const { sensor_id } = req.params;
      const { startDate, endDate } = req.query;
      
      const summary = await DataService.getSensorSummary(sensor_id, { startDate, endDate });
      
      res.json({
        sensor_id,
        period: { startDate, endDate },
        summary
      });
    } catch (error) {
      next(error);
    }
  }

  static async getDailyAggregations(req, res, next) {
    try {
      const { reading_type, startDate, endDate } = req.query;
      
      const aggregations = await DataService.getDailyAggregations({ 
        reading_type, 
        startDate, 
        endDate 
      });
      
      res.json({
        reading_type,
        period: { startDate, endDate },
        aggregations,
        total_days: aggregations.length
      });
    } catch (error) {
      next(error);
    }
  }

  static async getAnomalies(req, res, next) {
    try {
      const filters = req.query;
      const result = await DataService.getAnomalies(filters);
      
      res.json({
        anomalies: result.data,
        pagination: {
          page: result.page,
          limit: result.limit,
          total: result.total,
          pages: Math.ceil(result.total / result.limit)
        },
        filters: filters
      });
    } catch (error) {
      next(error);
    }
  }

  static async exportData(req, res, next) {
    try {
      const { format, ...filters } = req.query;
      
      if (!format) {
        return res.status(400).json({
          error: 'Export format is required',
          supported_formats: ['csv', 'json', 'parquet']
        });
      }

      const result = await DataService.exportData(format, filters);
      
      // Set appropriate headers for file download
      const filename = `agricultural_data_${Date.now()}.${format}`;
      
      if (format === 'csv') {
        res.setHeader('Content-Type', 'text/csv');
      } else if (format === 'json') {
        res.setHeader('Content-Type', 'application/json');
      } else if (format === 'parquet') {
        res.setHeader('Content-Type', 'application/octet-stream');
      }
      
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      
      if (typeof result === 'string') {
        res.send(result);
      } else {
        res.sendFile(result.filePath);
      }
    } catch (error) {
      next(error);
    }
  }
}

module.exports = DataController;
