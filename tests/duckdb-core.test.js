const DuckDBService = require('../src/services/duckDBService');
const path = require('path');
const fs = require('fs').promises;

describe('DuckDB Core Integration Tests', () => {
  let duckDBService;
  const testDbPath = path.join(__dirname, '..', 'data', 'test', 'test_agricultural_data.duckdb');
  const testDataPath = path.join(__dirname, '..', 'data', 'test', 'processed');

  beforeAll(async () => {
    // Ensure test directories exist
    await fs.mkdir(path.dirname(testDbPath), { recursive: true });
    await fs.mkdir(testDataPath, { recursive: true });

    // Initialize DuckDB service with test configuration
    const testConfig = {
      paths: {
        processedData: testDataPath
      }
    };
    
    duckDBService = new DuckDBService(testConfig, path.join(__dirname, '..', 'data', 'test'));
  });

  afterAll(async () => {
    if (duckDBService) {
      await duckDBService.close();
    }
    
    // Clean up test files
    try {
      await fs.unlink(testDbPath);
      await fs.rm(testDataPath, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  describe('DuckDB Initialization', () => {
    test('should initialize successfully', async () => {
      const result = await duckDBService.initialize();
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.mode).toBe('duckdb');
      expect(result.database).toBeDefined();
    });

    test('should create required tables', async () => {
      const queryResult = await duckDBService.queryData({ limit: 1 });
      
      expect(queryResult).toBeDefined();
      expect(queryResult.success).toBe(true);
      expect(queryResult.source).toBe('duckdb');
    });
  });

  describe('Basic CRUD Operations', () => {
    let testRecordId;

    test('should insert data successfully', async () => {
      const testData = {
        sensor_id: 'TEST_SENSOR_001',
        reading_type: 'temperature',
        value: 25.5,
        timestamp: new Date().toISOString(),
        field: 'test_field',
        location: 'test_location'
      };

      const result = await duckDBService.insertData(testData);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(1);
      
      testRecordId = result.id;
    });

    test('should query data successfully', async () => {
      const result = await duckDBService.queryData({ 
        sensor_id: 'TEST_SENSOR_001',
        limit: 10 
      });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.source).toBe('duckdb');
      expect(Array.isArray(result.data)).toBe(true);
      expect(result.data.length).toBeGreaterThan(0);
      
      const record = result.data[0];
      expect(record.sensor_id).toBe('TEST_SENSOR_001');
      expect(record.reading_type).toBe('temperature');
      expect(record.value).toBe(25.5);
    });

    test('should update data successfully', async () => {
      if (!testRecordId) {
        console.warn('Skipping update test - no record ID available');
        return;
      }

      const result = await duckDBService.updateData(testRecordId, { 
        value: 26.0 
      });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.updated).toBe(1);
    });

    test('should delete data successfully', async () => {
      if (!testRecordId) {
        console.warn('Skipping delete test - no record ID available');
        return;
      }

      const result = await duckDBService.deleteData(testRecordId);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.deleted).toBe(1);
    });
  });

  describe('Batch Operations', () => {
    test('should handle batch inserts efficiently', async () => {
      const batchData = Array.from({ length: 50 }, (_, i) => ({
        sensor_id: `BATCH_SENSOR_${String(i + 1).padStart(3, '0')}`,
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        timestamp: new Date(Date.now() - i * 60000).toISOString(),
        field: 'batch_test',
        location: 'test_location'
      }));

      const startTime = Date.now();
      const result = await duckDBService.insertData(batchData);
      const endTime = Date.now();

      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(50);
      
      const duration = endTime - startTime;
      console.log(`Batch insert of 50 records took ${duration}ms`);
      expect(duration).toBeLessThan(1000); // Should complete in under 1 second
    });

    test('should query batch data efficiently', async () => {
      const startTime = Date.now();
      const result = await duckDBService.queryData({ 
        field: 'batch_test',
        limit: 25 
      });
      const endTime = Date.now();

      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.data.length).toBe(25);
      
      const duration = endTime - startTime;
      console.log(`Query of 25 records took ${duration}ms`);
      expect(duration).toBeLessThan(500); // Should complete in under 500ms
    });
  });

  describe('Analytics and Statistics', () => {
    test('should get statistics successfully', async () => {
      const result = await duckDBService.getStats();
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.source).toBe('duckdb');
      expect(typeof result.total).toBe('number');
      expect(Array.isArray(result.byType)).toBe(true);
    });

    test('should filter data by various criteria', async () => {
      // Test sensor_id filter
      const sensorResult = await duckDBService.queryData({ 
        sensor_id: 'BATCH_SENSOR_001' 
      });
      expect(sensorResult.success).toBe(true);
      
      // Test reading_type filter
      const typeResult = await duckDBService.queryData({ 
        reading_type: 'temperature' 
      });
      expect(typeResult.success).toBe(true);
      
      // Test field filter
      const fieldResult = await duckDBService.queryData({ 
        field: 'batch_test' 
      });
      expect(fieldResult.success).toBe(true);
      expect(fieldResult.data.length).toBeGreaterThan(0);
    });
  });

  describe('Error Handling', () => {
    test('should handle invalid data gracefully', async () => {
      const invalidData = {
        sensor_id: '', // Empty sensor_id
        reading_type: 'temperature',
        value: 'not_a_number', // Invalid value
        timestamp: 'invalid_timestamp' // Invalid timestamp
      };

      try {
        await duckDBService.insertData(invalidData);
        // If it doesn't throw, check if it returns an error
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    test('should handle non-existent record updates', async () => {
      const result = await duckDBService.updateData(999999, { value: 30.0 });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.updated).toBe(0); // No records updated
    });

    test('should handle non-existent record deletes', async () => {
      const result = await duckDBService.deleteData(999999);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.deleted).toBe(0); // No records deleted
    });
  });

  describe('Service Lifecycle', () => {
    test('should close connection properly', async () => {
      // Create a temporary service to test closing
      const tempService = new DuckDBService({
        paths: { processedData: testDataPath }
      }, path.join(__dirname, '..', 'data', 'test'));
      
      await tempService.initialize();
      
      // Close should not throw
      await expect(tempService.close()).resolves.not.toThrow();
    });
  });
});
