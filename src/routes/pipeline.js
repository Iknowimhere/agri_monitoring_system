const express = require('express');
const multer = require('multer');
const path = require('path');
const router = express.Router();
const PipelineController = require('../controllers/pipelineController');
const { validate, dateRangeSchema } = require('../middleware/validation');
const config = require('../config/config');

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, config.paths.rawData);
  },
  filename: function (req, file, cb) {
    // Keep original filename or generate date-based name
    const originalName = file.originalname;
    const ext = path.extname(originalName);
    
    // If filename follows YYYY-MM-DD pattern, keep it
    if (/^\d{4}-\d{2}-\d{2}\.parquet$/.test(originalName)) {
      cb(null, originalName);
    } else {
      // Generate date-based filename
      const today = new Date().toISOString().split('T')[0];
      cb(null, `${today}.parquet`);
    }
  }
});

const upload = multer({
  storage: storage,
  limits: {
    fileSize: config.processing.maxFileSizeMB * 1024 * 1024 // Convert MB to bytes
  },
  fileFilter: (req, file, cb) => {
    if (path.extname(file.originalname).toLowerCase() !== '.parquet') {
      return cb(new Error('Only .parquet files are allowed'));
    }
    cb(null, true);
  }
});

/**
 * @swagger
 * components:
 *   schemas:
 *     PipelineStatus:
 *       type: object
 *       properties:
 *         status:
 *           type: string
 *           enum: [idle, running, completed, failed]
 *         stage:
 *           type: string
 *         progress:
 *           type: number
 *           minimum: 0
 *           maximum: 100
 *         startTime:
 *           type: string
 *           format: date-time
 *         endTime:
 *           type: string
 *           format: date-time
 *         statistics:
 *           type: object
 */

/**
 * @swagger
 * /api/pipeline/status:
 *   get:
 *     summary: Get current pipeline status
 *     tags: [Pipeline]
 *     responses:
 *       200:
 *         description: Pipeline status
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PipelineStatus'
 */
router.get('/status', PipelineController.getStatus);

/**
 * @swagger
 * /api/pipeline/run:
 *   post:
 *     summary: Run the complete data pipeline
 *     tags: [Pipeline]
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               startDate:
 *                 type: string
 *                 pattern: '^\d{4}-\d{2}-\d{2}$'
 *                 description: Start date for processing (YYYY-MM-DD)
 *               endDate:
 *                 type: string
 *                 pattern: '^\d{4}-\d{2}-\d{2}$'
 *                 description: End date for processing (YYYY-MM-DD)
 *               forceReprocess:
 *                 type: boolean
 *                 description: Force reprocessing of already processed files
 *     responses:
 *       202:
 *         description: Pipeline started successfully
 *       400:
 *         description: Invalid request parameters
 *       409:
 *         description: Pipeline is already running
 */
router.post('/run', validate(dateRangeSchema), PipelineController.runPipeline);

/**
 * @swagger
 * /api/pipeline/upload:
 *   post:
 *     summary: Upload raw data file(s)
 *     tags: [Pipeline]
 *     requestBody:
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               files:
 *                 type: array
 *                 items:
 *                   type: string
 *                   format: binary
 *                 description: Parquet files to upload
 *     responses:
 *       200:
 *         description: Files uploaded successfully
 *       400:
 *         description: Invalid file format or size
 */
router.post('/upload', upload.array('files', 10), PipelineController.uploadFiles);

/**
 * @swagger
 * /api/pipeline/ingest:
 *   post:
 *     summary: Run ingestion stage only
 *     tags: [Pipeline]
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               startDate:
 *                 type: string
 *                 pattern: '^\d{4}-\d{2}-\d{2}$'
 *               endDate:
 *                 type: string
 *                 pattern: '^\d{4}-\d{2}-\d{2}$'
 *     responses:
 *       200:
 *         description: Ingestion completed successfully
 */
router.post('/ingest', validate(dateRangeSchema), PipelineController.runIngestion);

/**
 * @swagger
 * /api/pipeline/transform:
 *   post:
 *     summary: Run transformation stage only
 *     tags: [Pipeline]
 *     responses:
 *       200:
 *         description: Transformation completed successfully
 */
router.post('/transform', PipelineController.runTransformation);

/**
 * @swagger
 * /api/pipeline/validate:
 *   post:
 *     summary: Run validation stage only
 *     tags: [Pipeline]
 *     responses:
 *       200:
 *         description: Validation completed successfully
 */
router.post('/validate', PipelineController.runValidation);

/**
 * @swagger
 * /api/pipeline/stop:
 *   post:
 *     summary: Stop the currently running pipeline
 *     tags: [Pipeline]
 *     responses:
 *       200:
 *         description: Pipeline stopped successfully
 *       400:
 *         description: No pipeline is currently running
 */
router.post('/stop', PipelineController.stopPipeline);

/**
 * @swagger
 * /api/pipeline/logs:
 *   get:
 *     summary: Get pipeline execution logs
 *     tags: [Pipeline]
 *     parameters:
 *       - in: query
 *         name: lines
 *         schema:
 *           type: integer
 *           minimum: 1
 *           maximum: 1000
 *           default: 100
 *         description: Number of log lines to retrieve
 *       - in: query
 *         name: level
 *         schema:
 *           type: string
 *           enum: [error, warn, info, debug]
 *         description: Filter logs by level
 *     responses:
 *       200:
 *         description: Pipeline logs
 */
router.get('/logs', PipelineController.getLogs);

module.exports = router;
