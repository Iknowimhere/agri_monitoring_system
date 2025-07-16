const express = require('express');
const router = express.Router();
const DataController = require('../controllers/dataController');
const { validate, queryParamsSchema, sensorDataSchema } = require('../middleware/validation');

/**
 * @swagger
 * /api/data/query:
 *   get:
 *     summary: Query processed sensor data
 *     tags: [Data]
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
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: Start date filter (YYYY-MM-DD)
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: End date filter (YYYY-MM-DD)
 *       - in: query
 *         name: anomalous
 *         schema:
 *           type: boolean
 *         description: Filter anomalous readings only
 *       - in: query
 *         name: minValue
 *         schema:
 *           type: number
 *         description: Minimum value filter
 *       - in: query
 *         name: maxValue
 *         schema:
 *           type: number
 *         description: Maximum value filter
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           minimum: 1
 *           default: 1
 *         description: Page number for pagination
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           minimum: 1
 *           maximum: 1000
 *           default: 100
 *         description: Number of records per page
 *       - in: query
 *         name: sortBy
 *         schema:
 *           type: string
 *           enum: [timestamp, sensor_id, reading_type, value]
 *           default: timestamp
 *         description: Sort field
 *       - in: query
 *         name: sortOrder
 *         schema:
 *           type: string
 *           enum: [asc, desc]
 *           default: desc
 *         description: Sort order
 *     responses:
 *       200:
 *         description: Query results
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/SensorData'
 *                 pagination:
 *                   type: object
 *                   properties:
 *                     page:
 *                       type: integer
 *                     limit:
 *                       type: integer
 *                     total:
 *                       type: integer
 *                     pages:
 *                       type: integer
 */
router.get('/query', DataController.queryData);

/**
 * @swagger
 * /api/data/sensors:
 *   get:
 *     summary: Get list of all sensors
 *     tags: [Data]
 *     responses:
 *       200:
 *         description: List of sensors
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 sensors:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       sensor_id:
 *                         type: string
 *                       last_reading:
 *                         type: string
 *                         format: date-time
 *                       reading_types:
 *                         type: array
 *                         items:
 *                           type: string
 *                       total_readings:
 *                         type: integer
 */
router.get('/sensors', DataController.getSensors);

/**
 * @swagger
 * /api/data/sensors/{sensor_id}/summary:
 *   get:
 *     summary: Get summary statistics for a specific sensor
 *     tags: [Data]
 *     parameters:
 *       - in: path
 *         name: sensor_id
 *         required: true
 *         schema:
 *           type: string
 *         description: Sensor ID
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: Start date for summary
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: End date for summary
 *     responses:
 *       200:
 *         description: Sensor summary statistics
 */
router.get('/sensors/:sensor_id/summary', DataController.getSensorSummary);

/**
 * @swagger
 * /api/data/aggregations/daily:
 *   get:
 *     summary: Get daily aggregations
 *     tags: [Data]
 *     parameters:
 *       - in: query
 *         name: reading_type
 *         schema:
 *           type: string
 *           enum: [temperature, humidity, soil_moisture, light_intensity, battery_level]
 *         description: Reading type to aggregate
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: Start date
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: End date
 *     responses:
 *       200:
 *         description: Daily aggregation results
 */
router.get('/aggregations/daily', DataController.getDailyAggregations);

/**
 * @swagger
 * /api/data/anomalies:
 *   get:
 *     summary: Get anomalous readings
 *     tags: [Data]
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
 *       - in: query
 *         name: startDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: Start date filter
 *       - in: query
 *         name: endDate
 *         schema:
 *           type: string
 *           pattern: '^\d{4}-\d{2}-\d{2}$'
 *         description: End date filter
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           minimum: 1
 *           default: 1
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           minimum: 1
 *           maximum: 1000
 *           default: 100
 *     responses:
 *       200:
 *         description: Anomalous readings
 */
router.get('/anomalies', DataController.getAnomalies);

/**
 * @swagger
 * /api/data/export:
 *   get:
 *     summary: Export data in various formats
 *     tags: [Data]
 *     parameters:
 *       - in: query
 *         name: format
 *         required: true
 *         schema:
 *           type: string
 *           enum: [csv, json, parquet]
 *         description: Export format
 *       - in: query
 *         name: sensor_id
 *         schema:
 *           type: string
 *       - in: query
 *         name: reading_type
 *         schema:
 *           type: string
 *           enum: [temperature, humidity, soil_moisture, light_intensity, battery_level]
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
 *         description: Exported data file
 */
router.get('/export', DataController.exportData);

module.exports = router;
