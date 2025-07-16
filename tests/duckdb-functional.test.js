const DuckDBService = require('../src/services/duckDBService');
const DataService = require('../src/services/dataService');
const path = require('path');

describe('DuckDB Functionality Tests', () => {
  let testDbPath;

  beforeAll(async () => {
    // Use a unique test database path
    testDbPath = path.join(__dirname, '..', 'data', 'test');
    
    // Initialize DataService (which includes DuckDB)
    await DataService.ensureInitialized();
  });

  afterAll(async () => {
    // Clean up
    await DataService.close();
  });

  describe('Database Status', () => {
    test('should have DuckDB initialized', async () => {
      const stats = await DataService.getStats();
      
      expect(stats).toBeDefined();
      expect(stats.source).toBe('duckdb');
      expect(typeof stats.total).toBe('number');
    });
  });

  describe('Data Operations', () => {
    test('should insert and query data successfully', async () => {
      // Create complete data record with all required fields
      const testData = {
        sensor_id: 'FUNC_TEST_SENSOR_001',
        timestamp: new Date().toISOString(),
        reading_type: 'temperature',
        value: 25.0,
        unit: 'celsius',
        field: 'test_field',
        location: {
          field: 'test_field',
          latitude: 40.7128,
          longitude: -74.0060
        },
        battery_level: 85,
        signal_strength: 75,
        data_quality: 'good',
        quality_score: 95
      };

      // Insert data
      const insertResult = await DataService.insertData(testData);
      expect(insertResult.success).toBe(true);

      // Query data back
      const queryResult = await DataService.queryData({ 
        sensor_id: 'FUNC_TEST_SENSOR_001' 
      });
      
      expect(queryResult.success).toBe(true);
      expect(queryResult.source).toBe('duckdb');
      expect(Array.isArray(queryResult.data)).toBe(true);
      expect(queryResult.data.length).toBeGreaterThan(0);

      const retrievedRecord = queryResult.data[0];
      expect(retrievedRecord.sensor_id).toBe('FUNC_TEST_SENSOR_001');
      expect(retrievedRecord.reading_type).toBe('temperature');
      expect(retrievedRecord.value).toBe(25.0);
    });

    test('should handle minimal data records', async () => {
      // Test with minimal required fields only
      const minimalData = {
        sensor_id: 'MINIMAL_SENSOR_001',
        timestamp: new Date().toISOString(),
        reading_type: 'humidity',
        value: 60.0,
        field: 'minimal_test'
      };

      const insertResult = await DataService.insertData(minimalData);
      expect(insertResult.success).toBe(true);

      const queryResult = await DataService.queryData({ 
        sensor_id: 'MINIMAL_SENSOR_001' 
      });
      
      expect(queryResult.success).toBe(true);
      expect(queryResult.data.length).toBeGreaterThan(0);
    });

    test('should get accurate statistics', async () => {
      const stats = await DataService.getStats();
      
      expect(stats.success).toBe(true);
      expect(stats.source).toBe('duckdb');
      expect(stats.total).toBeGreaterThan(0);
      expect(Array.isArray(stats.byType)).toBe(true);
    });

    test('should export data correctly', async () => {
      // Test JSON export
      const jsonData = await DataService.exportData('json', { 
        sensor_id: 'FUNC_TEST_SENSOR_001' 
      });
      
      expect(typeof jsonData).toBe('string');
      const parsedData = JSON.parse(jsonData);
      expect(Array.isArray(parsedData)).toBe(true);
      expect(parsedData.length).toBeGreaterThan(0);

      // Test CSV export
      const csvData = await DataService.exportData('csv', { 
        sensor_id: 'FUNC_TEST_SENSOR_001' 
      });
      
      expect(typeof csvData).toBe('string');
      expect(csvData.includes('sensor_id')).toBe(true);
    });
  });

  describe('Query Functionality', () => {
    test('should filter by reading type', async () => {
      const result = await DataService.queryData({ 
        reading_type: 'temperature' 
      });
      
      expect(result.success).toBe(true);
      expect(result.source).toBe('duckdb');
    });

    test('should filter by field', async () => {
      const result = await DataService.queryData({ 
        field: 'test_field' 
      });
      
      expect(result.success).toBe(true);
      expect(result.source).toBe('duckdb');
    });

    test('should handle pagination', async () => {
      const result = await DataService.queryData({ 
        page: 1,
        limit: 5 
      });
      
      expect(result.success).toBe(true);
      expect(result.page).toBe(1);
      expect(result.limit).toBe(5);
      expect(result.data.length).toBeLessThanOrEqual(5);
    });
  });

  describe('Sensor Operations', () => {
    test('should get sensors list', async () => {
      const sensors = await DataService.getSensors();
      
      expect(Array.isArray(sensors)).toBe(true);
      expect(sensors.length).toBeGreaterThan(0);
      
      // Should include our test sensor
      const testSensor = sensors.find(s => s.sensor_id === 'FUNC_TEST_SENSOR_001');
      expect(testSensor).toBeDefined();
      expect(testSensor.reading_types).toContain('temperature');
    });

    test('should get sensor summary', async () => {
      const summary = await DataService.getSensorSummary('FUNC_TEST_SENSOR_001');
      
      expect(summary).toBeDefined();
      expect(summary.sensor_id).toBe('FUNC_TEST_SENSOR_001');
      expect(summary.total_readings).toBeGreaterThan(0);
      expect(summary.reading_types).toContain('temperature');
    });
  });

  describe('Performance', () => {
    test('should handle batch operations efficiently', async () => {
      const batchData = Array.from({ length: 20 }, (_, i) => ({
        sensor_id: `BATCH_SENSOR_${String(i + 1).padStart(3, '0')}`,
        timestamp: new Date(Date.now() - i * 60000).toISOString(),
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        field: 'batch_test',
        unit: 'celsius',
        quality_score: 90
      }));

      const startTime = Date.now();
      
      // Insert records one by one to test performance
      let successCount = 0;
      for (const record of batchData) {
        try {
          const result = await DataService.insertData(record);
          if (result.success) successCount++;
        } catch (error) {
          console.warn(`Failed to insert record: ${error.message}`);
        }
      }
      
      const endTime = Date.now();
      const duration = endTime - startTime;
      
      console.log(`Batch processing of ${successCount}/${batchData.length} records took ${duration}ms`);
      expect(successCount).toBeGreaterThan(0);
      expect(duration).toBeLessThan(5000); // Should complete in under 5 seconds
    });

    test('should query efficiently', async () => {
      const startTime = Date.now();
      const result = await DataService.queryData({ 
        field: 'batch_test',
        limit: 10 
      });
      const endTime = Date.now();
      
      const duration = endTime - startTime;
      console.log(`Query took ${duration}ms`);
      
      expect(result.success).toBe(true);
      expect(duration).toBeLessThan(1000); // Should complete in under 1 second
    });
  });
});
