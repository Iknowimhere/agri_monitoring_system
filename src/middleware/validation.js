const Joi = require('joi');

const sensorDataSchema = Joi.object({
  sensor_id: Joi.string().required(),
  timestamp: Joi.string().isoDate().required(),
  reading_type: Joi.string().valid('temperature', 'humidity', 'soil_moisture', 'light_intensity', 'battery_level').required(),
  value: Joi.number().required(),
  battery_level: Joi.number().min(0).max(100).required()
});

const dateRangeSchema = Joi.object({
  startDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).required(),
  endDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).required()
});

const paginationSchema = Joi.object({
  page: Joi.alternatives().try(Joi.number().integer().min(1), Joi.string().pattern(/^\d+$/).custom((value) => parseInt(value, 10))).optional(),
  limit: Joi.alternatives().try(Joi.number().integer().min(1).max(1000), Joi.string().pattern(/^\d+$/).custom((value) => parseInt(value, 10))).optional(),
  sortBy: Joi.string().valid('timestamp', 'sensor_id', 'reading_type', 'value').optional(),
  sortOrder: Joi.string().valid('asc', 'desc').optional()
});

const queryParamsSchema = Joi.object({
  sensor_id: Joi.string().optional(),
  reading_type: Joi.string().valid('temperature', 'humidity', 'soil_moisture', 'light_intensity', 'battery_level').optional(),
  startDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).optional(),
  endDate: Joi.string().pattern(/^\d{4}-\d{2}-\d{2}$/).optional(),
  anomalous: Joi.boolean().optional(),
  minValue: Joi.number().optional(),
  maxValue: Joi.number().optional()
}).concat(paginationSchema);

const validate = (schema) => {
  return (req, res, next) => {
    const { error, value } = schema.validate(req.body || req.query, { 
      abortEarly: false,
      stripUnknown: true,
      allowUnknown: false
    });

    if (error) {
      const validationError = new Error('Validation Error');
      validationError.name = 'ValidationError';
      validationError.details = error.details.map(detail => ({
        field: detail.path.join('.'),
        message: detail.message
      }));
      return next(validationError);
    }

    // Merge validated values back to request
    if (req.body && Object.keys(req.body).length > 0) {
      req.body = value;
    } else {
      req.query = value;
    }

    next();
  };
};

module.exports = {
  sensorDataSchema,
  dateRangeSchema,
  paginationSchema,
  queryParamsSchema,
  validate
};
