const logger = require('../utils/logger');
const ReportsService = require('../services/reportsService');
const FileUtils = require('../utils/fileUtils');
const path = require('path');

class ReportsController {
  static async getQualityReport(req, res, next) {
    try {
      const { startDate, endDate, format = 'json' } = req.query;
      
      const report = await ReportsService.generateQualityReport({ startDate, endDate });
      
      if (format === 'csv') {
        const csvData = await ReportsService.convertReportToCSV(report);
        const filename = `quality_report_${Date.now()}.csv`;
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send(csvData);
      } else {
        res.json(report);
      }
    } catch (error) {
      next(error);
    }
  }

  static async getProcessingReport(req, res, next) {
    try {
      const { startDate, endDate } = req.query;
      
      const report = await ReportsService.generateProcessingReport({ startDate, endDate });
      
      res.json(report);
    } catch (error) {
      next(error);
    }
  }

  static async getCoverageReport(req, res, next) {
    try {
      const { sensor_id, reading_type } = req.query;
      
      const report = await ReportsService.generateCoverageReport({ sensor_id, reading_type });
      
      res.json(report);
    } catch (error) {
      next(error);
    }
  }

  static async getAnomalyReport(req, res, next) {
    try {
      const { startDate, endDate, threshold } = req.query;
      
      const report = await ReportsService.generateAnomalyReport({ 
        startDate, 
        endDate, 
        threshold: threshold ? parseFloat(threshold) : undefined 
      });
      
      res.json(report);
    } catch (error) {
      next(error);
    }
  }

  static async listReports(req, res, next) {
    try {
      const reports = await ReportsService.listGeneratedReports();
      
      res.json({
        reports,
        total_reports: reports.length
      });
    } catch (error) {
      next(error);
    }
  }

  static async downloadReport(req, res, next) {
    try {
      const { report_id } = req.params;
      
      const reportPath = await ReportsService.getReportPath(report_id);
      
      if (!(await FileUtils.fileExists(reportPath))) {
        return res.status(404).json({
          error: 'Report not found',
          report_id
        });
      }

      // Determine content type based on file extension
      const ext = FileUtils.getExtension(reportPath);
      let contentType = 'application/octet-stream';
      
      if (ext === '.csv') {
        contentType = 'text/csv';
      } else if (ext === '.json') {
        contentType = 'application/json';
      } else if (ext === '.html') {
        contentType = 'text/html';
      }

      res.setHeader('Content-Type', contentType);
      res.setHeader('Content-Disposition', `attachment; filename="${path.basename(reportPath)}"`);
      res.sendFile(path.resolve(reportPath));
    } catch (error) {
      next(error);
    }
  }
}

module.exports = ReportsController;
