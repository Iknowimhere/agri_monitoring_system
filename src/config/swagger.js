const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Agricultural Data Pipeline API',
      version: '1.0.0',
      description: 'Production-grade API for agricultural sensor data processing pipeline',
      contact: {
        name: 'SatSure Team',
        email: 'aravindhan@satsure.co'
      }
    },
    servers: [
      {
        url: 'http://localhost:3000',
        description: 'Development server'
      }
    ],
    components: {
      schemas: {
        SensorData: {
          type: 'object',
          properties: {
            sensor_id: {
              type: 'string',
              description: 'Unique identifier for the sensor'
            },
            timestamp: {
              type: 'string',
              format: 'date-time',
              description: 'ISO datetime of the reading'
            },
            reading_type: {
              type: 'string',
              enum: ['temperature', 'humidity', 'soil_moisture', 'light_intensity', 'battery_level'],
              description: 'Type of sensor reading'
            },
            value: {
              type: 'number',
              description: 'Sensor reading value'
            },
            battery_level: {
              type: 'number',
              minimum: 0,
              maximum: 100,
              description: 'Battery level percentage'
            }
          },
          required: ['sensor_id', 'timestamp', 'reading_type', 'value', 'battery_level']
        },
        PipelineStatus: {
          type: 'object',
          properties: {
            status: {
              type: 'string',
              enum: ['idle', 'running', 'completed', 'failed']
            },
            stage: {
              type: 'string',
              description: 'Current processing stage'
            },
            progress: {
              type: 'number',
              minimum: 0,
              maximum: 100,
              description: 'Progress percentage'
            },
            startTime: {
              type: 'string',
              format: 'date-time'
            },
            endTime: {
              type: 'string',
              format: 'date-time'
            },
            statistics: {
              type: 'object',
              properties: {
                filesProcessed: { type: 'number' },
                recordsProcessed: { type: 'number' },
                recordsSkipped: { type: 'number' },
                errors: { type: 'number' }
              }
            }
          }
        },
        DataQualityReport: {
          type: 'object',
          properties: {
            metric: {
              type: 'string',
              description: 'Quality metric name'
            },
            reading_type: {
              type: 'string',
              description: 'Type of sensor reading'
            },
            sensor_id: {
              type: 'string',
              description: 'Sensor identifier'
            },
            value: {
              type: 'number',
              description: 'Metric value'
            },
            timestamp: {
              type: 'string',
              format: 'date-time',
              description: 'Report generation timestamp'
            }
          }
        },
        Error: {
          type: 'object',
          properties: {
            error: {
              type: 'string',
              description: 'Error type'
            },
            message: {
              type: 'string',
              description: 'Error message'
            },
            code: {
              type: 'number',
              description: 'Error code'
            },
            timestamp: {
              type: 'string',
              format: 'date-time'
            }
          }
        }
      }
    }
  },
  apis: ['./src/routes/*.js'] // Path to the API docs
};

const specs = swaggerJsdoc(options);
module.exports = specs;
