const logger = require('../utils/logger');
const PipelineServiceClass = require('../services/pipelineService');
const FileUtils = require('../utils/fileUtils');
const config = require('../config/config');
const path = require('path');

// Create singleton instance of PipelineService
const PipelineService = new PipelineServiceClass();

class PipelineController {
  static async getStatus(req, res, next) {
    try {
      const status = await PipelineService.getStatus();
      res.json(status);
    } catch (error) {
      next(error);
    }
  }

  static async runPipeline(req, res, next) {
    try {
      const { startDate, endDate, forceReprocess = false } = req.body;
      
      // Check if pipeline is already running
      const currentStatus = await PipelineService.getStatus();
      if (currentStatus.status === 'running') {
        return res.status(409).json({
          error: 'Pipeline is already running',
          current_status: currentStatus
        });
      }

      // Start pipeline asynchronously
      PipelineService.runFullPipeline({ startDate, endDate, forceReprocess })
        .catch(error => {
          logger.error('Pipeline execution failed:', error);
        });

      res.status(202).json({
        message: 'Pipeline started successfully',
        startDate,
        endDate,
        forceReprocess,
        status_endpoint: '/api/pipeline/status'
      });
    } catch (error) {
      next(error);
    }
  }

  static async uploadFiles(req, res, next) {
    try {
      if (!req.files || req.files.length === 0) {
        return res.status(400).json({
          error: 'No files uploaded'
        });
      }

      const uploadedFiles = [];
      const errors = [];

      for (const file of req.files) {
        try {
          // Validate file size
          const fileSizeMB = await FileUtils.getFileSizeMB(file.path);
          if (fileSizeMB > config.processing.maxFileSizeMB) {
            errors.push({
              filename: file.originalname,
              error: `File size ${fileSizeMB.toFixed(2)}MB exceeds limit of ${config.processing.maxFileSizeMB}MB`
            });
            await FileUtils.deleteFile(file.path);
            continue;
          }

          uploadedFiles.push({
            filename: file.filename,
            originalname: file.originalname,
            size: file.size,
            path: file.path
          });

          logger.info(`File uploaded successfully: ${file.originalname}`);
        } catch (error) {
          errors.push({
            filename: file.originalname,
            error: error.message
          });
        }
      }

      res.json({
        message: 'File upload completed',
        uploaded: uploadedFiles,
        errors: errors,
        total_uploaded: uploadedFiles.length,
        total_errors: errors.length
      });
    } catch (error) {
      next(error);
    }
  }

  static async runIngestion(req, res, next) {
    try {
      const { startDate, endDate } = req.body;
      
      const result = await PipelineService.runIngestion({ startDate, endDate });
      
      res.json({
        message: 'Ingestion completed successfully',
        ...result
      });
    } catch (error) {
      next(error);
    }
  }

  static async runTransformation(req, res, next) {
    try {
      const result = await PipelineService.runTransformation();
      
      res.json({
        message: 'Transformation completed successfully',
        ...result
      });
    } catch (error) {
      next(error);
    }
  }

  static async runValidation(req, res, next) {
    try {
      const result = await PipelineService.runValidation();
      
      res.json({
        message: 'Validation completed successfully',
        ...result
      });
    } catch (error) {
      next(error);
    }
  }

  static async stopPipeline(req, res, next) {
    try {
      const result = await PipelineService.stopPipeline();
      
      if (!result.success) {
        return res.status(400).json({
          error: 'No pipeline is currently running'
        });
      }

      res.json({
        message: 'Pipeline stopped successfully',
        stopped_at: result.stoppedAt
      });
    } catch (error) {
      next(error);
    }
  }

  static async getLogs(req, res, next) {
    try {
      const { lines = 100, level } = req.query;
      
      const logs = await PipelineService.getLogs({ lines: parseInt(lines), level });
      
      res.json({
        logs,
        total_lines: logs.length,
        filtered_by_level: level || 'all'
      });
    } catch (error) {
      next(error);
    }
  }
}

module.exports = PipelineController;
