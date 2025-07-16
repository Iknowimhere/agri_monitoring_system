const logger = require('../utils/logger');

const errorHandler = (err, req, res, next) => {
  logger.error('Error occurred:', {
    error: err.message,
    stack: err.stack,
    url: req.url,
    method: req.method,
    ip: req.ip,
    timestamp: new Date().toISOString()
  });

  // Default error response
  let statusCode = 500;
  let message = 'Internal Server Error';
  let details = null;

  // Handle specific error types
  if (err.name === 'ValidationError') {
    statusCode = 400;
    message = 'Validation Error';
    details = err.details;
  } else if (err.name === 'MulterError') {
    statusCode = 400;
    message = 'File Upload Error';
    details = err.message;
  } else if (err.code === 'ENOENT') {
    statusCode = 404;
    message = 'File Not Found';
  } else if (err.code === 'EACCES') {
    statusCode = 403;
    message = 'Permission Denied';
  } else if (err.message) {
    message = err.message;
  }

  // Don't expose stack traces in production
  const response = {
    error: message,
    timestamp: new Date().toISOString(),
    path: req.url,
    method: req.method
  };

  if (process.env.NODE_ENV !== 'production') {
    response.stack = err.stack;
  }

  if (details) {
    response.details = details;
  }

  res.status(statusCode).json(response);
};

module.exports = errorHandler;
