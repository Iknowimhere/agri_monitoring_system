const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./config/swagger');
const logger = require('./utils/logger');
const errorHandler = require('./middleware/errorHandler');
const config = require('./config/config');

// Import routes
const pipelineRoutes = require('./routes/pipeline');
const dataRoutes = require('./routes/data');
const reportsRoutes = require('./routes/reports');
const healthRoutes = require('./routes/health');

const app = express();

// Security middleware
app.use(helmet());
app.use(cors());

// Rate limiting
const limiter = rateLimit({
  windowMs: config.api.rateLimitWindowMs,
  max: config.api.rateLimitMaxRequests,
  message: 'Too many requests from this IP, please try again later.'
});
app.use('/api/', limiter);

// Body parsing middleware
app.use(compression());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// API Documentation
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// Request logging
app.use((req, res, next) => {
  logger.info(`${req.method} ${req.url}`, {
    ip: req.ip,
    userAgent: req.get('User-Agent'),
    timestamp: new Date().toISOString()
  });
  next();
});

// Routes
app.use('/health', healthRoutes);
app.use('/api/pipeline', pipelineRoutes);
app.use('/api/data', dataRoutes);
app.use('/api/reports', reportsRoutes);

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Agricultural Data Pipeline API',
    version: '1.0.0',
    documentation: '/api-docs',
    endpoints: {
      health: '/health',
      pipeline: '/api/pipeline',
      data: '/api/data',
      reports: '/api/reports'
    }
  });
});

// Static files (after API routes)
app.use(express.static('public'));

// Error handling
app.use(errorHandler);

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    message: `Cannot ${req.method} ${req.originalUrl}`
  });
});

const PORT = config.server.port;

// Only start server if not in test environment
if (process.env.NODE_ENV !== 'test') {
  app.listen(PORT, () => {
    logger.info(`Agricultural Data Pipeline API server running on port ${PORT}`);
    logger.info(`API Documentation available at http://localhost:${PORT}/api-docs`);
  });
}

module.exports = app;
