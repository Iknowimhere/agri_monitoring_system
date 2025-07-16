const express = require('express');
const router = express.Router();
const ReportsController = require('../controllers/reportsController');

/**
 * @swagger
 * /api/reports/quality:
 *   get:
 *     summary: Get data quality report
 *     tags: [Reports]
 *     parameters:
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: Start date for report
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: End date for report
 *       - in: query
 *         name: format
 *         schema:
 *           type: string
 *           enum: [json, csv]
 *           default: json
 *         description: Report format
 *     responses:
 *       200:
 *         description: Data quality report
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 report_id:
 *                   type: string
 *                 generated_at:
 *                   type: string
 *                   format: date-time
 *                 period:
 *                   type: object
 *                   properties:
 *                     start_date:
 *                       type: string
 *                     end_date:
 *                       type: string
 *                 metrics:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/DataQualityReport'
 */
router.get('/quality', ReportsController.getQualityReport);

/**
 * @swagger
 * /api/reports/processing:
 *   get:
 *     summary: Get processing statistics report
 *     tags: [Reports]
 *     parameters:
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *     responses:
 *       200:
 *         description: Processing statistics
 */
router.get('/processing', ReportsController.getProcessingReport);

/**
 * @swagger
 * /api/reports/coverage:
 *   get:
 *     summary: Get data coverage report
 *     tags: [Reports]
 *     parameters:
 *       - in: query
 *         name: sensor_id
 *         schema:
 *           type: string
 *         description: Filter by sensor ID
 *       - in: query
 *         name: reading_type
 *         schema:
 *           type: string
 *           enum: [temperature, humidity, soil_moisture, light_intensity, battery_level]
 *         description: Filter by reading type
 *     responses:
 *       200:
 *         description: Data coverage analysis
 */
router.get('/coverage', ReportsController.getCoverageReport);

/**
 * @swagger
 * /api/reports/anomalies:
 *   get:
 *     summary: Get anomaly detection report
 *     tags: [Reports]
 *     parameters:
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *       - in: query
 *         name: threshold
 *         schema:
 *           type: number
 *           minimum: 0
 *           maximum: 100
 *         description: Anomaly percentage threshold
 *     responses:
 *       200:
 *         description: Anomaly detection results
 */
router.get('/anomalies', ReportsController.getAnomalyReport);

/**
 * @swagger
 * /api/reports/list:
 *   get:
 *     summary: Get list of generated reports
 *     tags: [Reports]
 *     responses:
 *       200:
 *         description: List of available reports
 */
router.get('/list', ReportsController.listReports);

/**
 * @swagger
 * /api/reports/{report_id}:
 *   get:
 *     summary: Download a specific report
 *     tags: [Reports]
 *     parameters:
 *       - in: path
 *         name: report_id
 *         required: true
 *         schema:
 *           type: string
 *         description: Report ID
 *     responses:
 *       200:
 *         description: Report file download
 *       404:
 *         description: Report not found
 */
router.get('/:report_id', ReportsController.downloadReport);

module.exports = router;
