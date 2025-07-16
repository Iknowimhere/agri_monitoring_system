const path = require('path');
const fs = require('fs');

class ValidationService {
  constructor(config = {}) {
    this.config = config;
    this.validationDataPath = path.join(process.cwd(), 'data', 'validation');
    
    // Ensure directory exists
    if (!fs.existsSync(this.validationDataPath)) {
      fs.mkdirSync(this.validationDataPath, { recursive: true });
    }
  }

  async validateData() {
    const validationResults = {
      totalRecords: 0,
      validRecords: 0,
      invalidRecords: 0,
      anomalies: [],
      qualityScore: 0,
      timestamp: new Date().toISOString()
    };

    try {
      const rawDataPath = path.join(process.cwd(), 'data', 'raw');
      if (!fs.existsSync(rawDataPath)) {
        return validationResults;
      }

      const files = fs.readdirSync(rawDataPath)
        .filter(f => f.endsWith('.json'))
        .map(f => path.join(rawDataPath, f));
      
      for (const file of files) {
        try {
          const data = JSON.parse(fs.readFileSync(file, 'utf8'));
          const records = Array.isArray(data) ? data : [data];
          
          for (const record of records) {
            validationResults.totalRecords++;
            
            if (this.validateRecord(record)) {
              validationResults.validRecords++;
            } else {
              validationResults.invalidRecords++;
            }
          }
        } catch (error) {
          console.error(`Failed to validate file ${file}:`, error);
        }
      }
      
      // Calculate quality score
      if (validationResults.totalRecords > 0) {
        validationResults.qualityScore = 
          (validationResults.validRecords / validationResults.totalRecords) * 100;
      }
      
      // Save validation results
      const reportPath = path.join(process.cwd(), 'data', 'reports', 
        `quality_${Date.now()}.json`);
      
      // Ensure reports directory exists
      const reportsDir = path.dirname(reportPath);
      if (!fs.existsSync(reportsDir)) {
        fs.mkdirSync(reportsDir, { recursive: true });
      }
      
      fs.writeFileSync(reportPath, JSON.stringify(validationResults, null, 2));
      
      return validationResults;
    } catch (error) {
      console.error('Data validation failed:', error);
      throw error;
    }
  }

  validateRecord(record) {
    // Check required fields
    if (!record || !record.timestamp || !record.sensor_id) {
      return false;
    }
    
    // Check value validity
    if (record.value === null || record.value === undefined) {
      return false;
    }
    
    // Check data types
    if (typeof record.value !== 'number' && typeof record.value !== 'string') {
      return false;
    }
    
    return true;
  }

  validateSchema(data) {
    return Array.isArray(data) || typeof data === 'object';
  }

  validateTimestamp(timestamp) {
    return typeof timestamp === 'string' && !isNaN(Date.parse(timestamp));
  }

  validateSensorReading(reading) {
    return typeof reading === 'number' && !isNaN(reading);
  }
}

module.exports = ValidationService;
