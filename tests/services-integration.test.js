const DataService = require('../src/services/dataService');
const PipelineService = require('../src/services/pipelineService');

describe('Basic Service Integration Tests', () => {
  beforeAll(async () => {
    // Initialize services
    await DataService.ensureInitialized();
  });

  afterAll(async () => {
    // Clean up
    await DataService.close();
    await PipelineService.close();
  });

  describe('DataService Integration', () => {
    test('should initialize and be ready', async () => {
      // DataService should be initialized from beforeAll
      const stats = await DataService.getStats();
      
      expect(stats).toBeDefined();
      expect(typeof stats.total).toBe('number');
    });

    test('should handle basic data operations', async () => {
      const testData = {
        sensor_id: 'INTEGRATION_SENSOR_001',
        reading_type: 'temperature',
        value: 24.0,
        timestamp: new Date().toISOString(),
        field: 'integration_test'
      };

      // Insert data
      const insertResult = await DataService.insertData(testData);
      expect(insertResult.success).toBe(true);

      // Query data
      const queryResult = await DataService.queryData({ 
        sensor_id: 'INTEGRATION_SENSOR_001' 
      });
      expect(queryResult.success).toBe(true);
      expect(queryResult.data.length).toBeGreaterThan(0);
    });

    test('should get sensors list', async () => {
      const sensors = await DataService.getSensors();
      
      expect(Array.isArray(sensors)).toBe(true);
      // Should have at least the test sensor we inserted
      expect(sensors.length).toBeGreaterThan(0);
    });

    test('should export data in different formats', async () => {
      // Test CSV export
      const csvData = await DataService.exportData('csv', { 
        sensor_id: 'INTEGRATION_SENSOR_001' 
      });
      expect(typeof csvData).toBe('string');
      expect(csvData).toContain('sensor_id');

      // Test JSON export
      const jsonData = await DataService.exportData('json', { 
        sensor_id: 'INTEGRATION_SENSOR_001' 
      });
      expect(typeof jsonData).toBe('string');
      const parsedData = JSON.parse(jsonData);
      expect(Array.isArray(parsedData)).toBe(true);
    });
  });

  describe('PipelineService Integration', () => {
    test('should process data through pipeline', async () => {
      const inputData = [
        {
          sensor_id: 'PIPELINE_SENSOR_001',
          reading_type: 'humidity',
          value: 65.0,
          timestamp: new Date().toISOString(),
          field: 'pipeline_test'
        },
        {
          sensor_id: 'PIPELINE_SENSOR_002',
          reading_type: 'soil_moisture',
          value: 45.0,
          timestamp: new Date().toISOString(),
          field: 'pipeline_test'
        }
      ];

      const result = await PipelineService.processData(inputData);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.processed).toBe(2);
      expect(result.validation).toBeDefined();
      expect(result.validation.passed).toBe(2);
      expect(result.validation.failed).toBe(0);
    });

    test('should get pipeline status', async () => {
      const status = await PipelineService.getStatus();
      
      expect(status).toBeDefined();
      expect(status.status).toBeDefined();
      expect(status.totalProcessed).toBeDefined();
      expect(typeof status.totalProcessed).toBe('number');
    });

    test('should validate data correctly', () => {
      const validData = {
        sensor_id: 'VALID_SENSOR',
        reading_type: 'temperature',
        value: 25.0,
        timestamp: new Date().toISOString(),
        field: 'test_field'
      };

      const validation = PipelineService.validateRecord(validData);
      expect(validation.isValid).toBe(true);
      expect(validation.errors).toHaveLength(0);

      const invalidData = {
        sensor_id: '', // Invalid empty sensor_id
        reading_type: 'temperature',
        value: 'not_a_number', // Invalid value
        timestamp: new Date().toISOString(),
        field: 'test_field'
      };

      const invalidValidation = PipelineService.validateRecord(invalidData);
      expect(invalidValidation.isValid).toBe(false);
      expect(invalidValidation.errors.length).toBeGreaterThan(0);
    });
  });

  describe('End-to-End Data Flow', () => {
    test('should handle complete data workflow', async () => {
      // Step 1: Process data through pipeline
      const inputData = [
        {
          sensor_id: 'E2E_SENSOR_001',
          reading_type: 'temperature',
          value: 23.5,
          timestamp: new Date().toISOString(),
          field: 'e2e_test'
        }
      ];

      const pipelineResult = await PipelineService.processData(inputData);
      expect(pipelineResult.success).toBe(true);
      expect(pipelineResult.processed).toBe(1);

      // Step 2: Query the processed data
      const queryResult = await DataService.queryData({ 
        sensor_id: 'E2E_SENSOR_001' 
      });
      expect(queryResult.success).toBe(true);
      expect(queryResult.data.length).toBeGreaterThan(0);

      // Step 3: Get sensor summary
      const sensorSummary = await DataService.getSensorSummary('E2E_SENSOR_001');
      expect(sensorSummary).toBeDefined();
      expect(sensorSummary.sensor_id).toBe('E2E_SENSOR_001');
      expect(sensorSummary.total_readings).toBeGreaterThan(0);

      // Step 4: Get aggregated data
      const aggregations = await DataService.getDailyAggregations({
        sensor_id: 'E2E_SENSOR_001',
        startDate: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        endDate: new Date().toISOString()
      });
      expect(aggregations.success).toBe(true);
      expect(aggregations.aggregations).toBeDefined();
    });
  });

  describe('Performance Tests', () => {
    test('should handle moderate dataset efficiently', async () => {
      const moderateDataset = Array.from({ length: 100 }, (_, i) => ({
        sensor_id: `PERF_SENSOR_${String(i % 10).padStart(2, '0')}`,
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        timestamp: new Date(Date.now() - i * 60000).toISOString(),
        field: 'performance_test'
      }));

      const startTime = Date.now();
      const result = await PipelineService.processData(moderateDataset);
      const endTime = Date.now();

      expect(result.success).toBe(true);
      expect(result.processed).toBe(100);
      
      const duration = endTime - startTime;
      console.log(`Pipeline processing of 100 records took ${duration}ms`);
      expect(duration).toBeLessThan(3000); // Should complete in under 3 seconds
    });

    test('should query large datasets efficiently', async () => {
      const startTime = Date.now();
      const result = await DataService.queryData({ 
        field: 'performance_test',
        limit: 50 
      });
      const endTime = Date.now();

      expect(result.success).toBe(true);
      expect(result.data.length).toBeGreaterThan(0);
      
      const duration = endTime - startTime;
      console.log(`Query of performance test data took ${duration}ms`);
      expect(duration).toBeLessThan(1000); // Should complete in under 1 second
    });
  });

  describe('Error Handling', () => {
    test('should handle service errors gracefully', async () => {
      // Test with invalid query parameters
      const result = await DataService.queryData({ 
        invalid_field: 'invalid_value' 
      });
      
      // Should still return a valid response structure
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
    });

    test('should handle malformed pipeline data', async () => {
      const malformedData = [
        { invalid: 'data' },
        { sensor_id: 'VALID_SENSOR', reading_type: 'temperature', value: 25.0 } // Missing required fields
      ];

      const result = await PipelineService.processData(malformedData);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.validation.failed).toBeGreaterThan(0);
      expect(Array.isArray(result.validation.errors)).toBe(true);
    });
  });
});
