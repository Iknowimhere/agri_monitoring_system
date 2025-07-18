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

// Import DuckDB service for global initialization
const DuckDBService = require('./services/duckDBService');
const duckDBSingleton = require('./services/duckDBSingleton');

// Import routes
const pipelineRoutes = require('./routes/pipeline');
const dataRoutes = require('./routes/data');
const reportsRoutes = require('./routes/reports');
const healthRoutes = require('./routes/health');

const app = express();

// Initialize DuckDB service globally
let globalDuckDBService = null;

async function initializeDatabase() {
  try {
    logger.info('Initializing global DuckDB service...');
    globalDuckDBService = await duckDBSingleton.getInstance();
    
    logger.info('Global DuckDB service initialized', { 
      mode: globalDuckDBService.dbType || 'fallback',
      success: globalDuckDBService.isAvailable 
    });
    
    // Make DuckDB service available globally
    app.locals.duckDBService = globalDuckDBService;
    
    return {
      success: globalDuckDBService.isAvailable,
      mode: globalDuckDBService.dbType || 'fallback'
    };
  } catch (error) {
    logger.error('Failed to initialize DuckDB service:', error);
    throw error;
  }
}

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

// 404 handler for non-existent endpoints
app.use((req, res, next) => {
  res.status(404).json({
    error: 'Endpoint not found',
    message: `The requested endpoint ${req.method} ${req.url} was not found.`,
    availableEndpoints: {
      health: '/health',
      pipeline: '/api/pipeline',
      data: '/api/data',
      reports: '/api/reports'
    }
  });
});

// Error handling middleware (must be last)
app.use(errorHandler);

// Export app and initialization function
module.exports = { app, initializeDatabase, getDuckDBService: () => globalDuckDBService };
