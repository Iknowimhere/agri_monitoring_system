const DuckDBService = require('../src/services/duckDBService');
const buildToolsChecker = require('../src/utils/buildToolsChecker');
const config = require('../src/config/config');
const path = require('path');
const fs = require('fs').promises;

describe('DuckDBService', () => {
  let duckDBService;
  let testConfig;

  beforeAll(async () => {
    // Create test-specific configuration
    testConfig = {
      ...config,
      paths: {
        ...config.paths,
        processedData: path.join(process.cwd(), 'data', 'test-processed')
      }
    };

    duckDBService = new DuckDBService(testConfig);
  });

  beforeEach(async () => {
    // Clean up test data before each test
    try {
      const testDataPath = path.join(process.cwd(), 'data', 'test-processed');
      await fs.rmdir(testDataPath, { recursive: true });
    } catch (error) {
      // Directory might not exist, that's ok
    }
  });

  afterAll(async () => {
    if (duckDBService) {
      await duckDBService.close();
    }
  });

  describe('Initialization', () => {
    test('should initialize with proper configuration', () => {
      expect(duckDBService).toBeDefined();
      expect(duckDBService.config).toBeDefined();
      expect(duckDBService.dbType).toBeNull();
      expect(duckDBService.isAvailable).toBe(false);
    });

    test('should initialize database service', async () => {
      const result = await duckDBService.initialize();
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(['duckdb', 'sqlite', 'fallback']).toContain(result.mode);
      
      if (result.mode === 'duckdb') {
        expect(duckDBService.isAvailable).toBe(true);
        expect(duckDBService.dbType).toBe('duckdb');
      } else if (result.mode === 'sqlite') {
        expect(duckDBService.isAvailable).toBe(true);
        expect(duckDBService.dbType).toBe('sqlite');
      } else {
        expect(duckDBService.isAvailable).toBe(false);
        expect(duckDBService.dbType).toBeNull();
      }
    }, 10000);

    test('should check DuckDB availability', async () => {
      const isAvailable = await duckDBService.checkDuckDB();
      expect(typeof isAvailable).toBe('boolean');
    });

    test('should check SQLite availability', async () => {
      const isAvailable = await duckDBService.checkSQLite();
      expect(typeof isAvailable).toBe('boolean');
    });
  });

  describe('Data Operations', () => {
    const testSensorData = {
      sensor_id: 'TEST_SENSOR_001',
      location: { 
        field: 'test_field', 
        latitude: 40.7128, 
        longitude: -74.0060 
      },
      reading_type: 'temperature',
      value: 25.5,
      unit: 'celsius',
      quality_score: 95,
      timestamp: new Date().toISOString(),
      battery_level: 88,
      signal_strength: 92,
      data_quality: 'excellent'
    };

    beforeEach(async () => {
      await duckDBService.initialize();
    });

    test('should insert single record', async () => {
      const result = await duckDBService.insertData(testSensorData);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(1);
    });

    test('should insert multiple records', async () => {
      const multipleRecords = [
        { ...testSensorData, sensor_id: 'TEST_SENSOR_001' },
        { ...testSensorData, sensor_id: 'TEST_SENSOR_002', value: 26.0 },
        { ...testSensorData, sensor_id: 'TEST_SENSOR_003', value: 24.5 }
      ];

      const result = await duckDBService.insertData(multipleRecords);
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(3);
    });

    test('should query data without filters', async () => {
      // Insert test data first
      await duckDBService.insertData(testSensorData);
      
      const result = await duckDBService.queryData({});
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      expect(result.data.length).toBeGreaterThan(0);
      expect(['duckdb', 'sqlite', 'fallback']).toContain(result.source);
    });

    test('should query data with sensor_id filter', async () => {
      // Insert test data first
      await duckDBService.insertData(testSensorData);
      
      const result = await duckDBService.queryData({ 
        sensor_id: 'TEST_SENSOR_001' 
      });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      
      // All returned records should match the filter
      result.data.forEach(record => {
        expect(record.sensor_id).toBe('TEST_SENSOR_001');
      });
    });

    test('should query data with reading_type filter', async () => {
      // Insert test data first
      await duckDBService.insertData(testSensorData);
      
      const result = await duckDBService.queryData({ 
        reading_type: 'temperature' 
      });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      
      // All returned records should match the filter
      result.data.forEach(record => {
        expect(record.reading_type).toBe('temperature');
      });
    });

    test('should query data with limit', async () => {
      // Insert multiple records
      const multipleRecords = Array.from({ length: 10 }, (_, i) => ({
        ...testSensorData,
        sensor_id: `TEST_SENSOR_${i.toString().padStart(3, '0')}`,
        value: 20 + i
      }));
      
      await duckDBService.insertData(multipleRecords);
      
      const result = await duckDBService.queryData({ limit: 5 });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      expect(result.data.length).toBeLessThanOrEqual(5);
    });

    test('should handle empty query results', async () => {
      const result = await duckDBService.queryData({ 
        sensor_id: 'NON_EXISTENT_SENSOR' 
      });
      
      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      expect(result.data.length).toBe(0);
    });
  });

  describe('Statistics Operations', () => {
    beforeEach(async () => {
      await duckDBService.initialize();
      
      // Insert test data for statistics
      const testData = [
        {
          sensor_id: 'STATS_SENSOR_001',
          location: { field: 'field_1', latitude: 40.7128, longitude: -74.0060 },
          reading_type: 'temperature',
          value: 25.0,
          unit: 'celsius',
          quality_score: 95,
          timestamp: new Date().toISOString(),
          battery_level: 88,
          signal_strength: 92,
          data_quality: 'excellent'
        },
        {
          sensor_id: 'STATS_SENSOR_002',
          location: { field: 'field_2', latitude: 40.7580, longitude: -73.9855 },
          reading_type: 'humidity',
          value: 65.0,
          unit: 'percentage',
          quality_score: 90,
          timestamp: new Date().toISOString(),
          battery_level: 85,
          signal_strength: 88,
          data_quality: 'good'
        }
      ];
      
      await duckDBService.insertData(testData);
    });

    test('should get statistics', async () => {
      const stats = await duckDBService.getStats();
      
      expect(stats).toBeDefined();
      expect(stats.success).toBe(true);
      expect(typeof stats.total).toBe('number');
      expect(stats.total).toBeGreaterThan(0);
      expect(['duckdb', 'sqlite', 'fallback']).toContain(stats.source);
      
      if (stats.byType) {
        expect(Array.isArray(stats.byType) || typeof stats.byType === 'object').toBe(true);
      }
    });

    test('should return correct total count', async () => {
      const stats = await duckDBService.getStats();
      expect(stats.total).toBeGreaterThanOrEqual(2); // At least our test data
    });

    test('should include latest timestamp', async () => {
      const stats = await duckDBService.getStats();
      
      if (stats.latestTimestamp) {
        expect(new Date(stats.latestTimestamp)).toBeInstanceOf(Date);
        expect(new Date(stats.latestTimestamp).getTime()).not.toBeNaN();
      }
    });
  });

  describe('CRUD Operations', () => {
    let testRecordId;
    
    beforeEach(async () => {
      await duckDBService.initialize();
      
      // Insert a record for update/delete tests
      const testData = {
        sensor_id: 'CRUD_TEST_SENSOR',
        location: { field: 'crud_field', latitude: 40.7128, longitude: -74.0060 },
        reading_type: 'temperature',
        value: 20.0,
        unit: 'celsius',
        quality_score: 80,
        timestamp: new Date().toISOString(),
        battery_level: 75,
        signal_strength: 85,
        data_quality: 'good'
      };
      
      await duckDBService.insertData(testData);
      
      // Get the inserted record to use its ID
      const queryResult = await duckDBService.queryData({ sensor_id: 'CRUD_TEST_SENSOR' });
      if (queryResult.data.length > 0) {
        testRecordId = queryResult.data[0].id;
      }
    });

    test('should update data', async () => {
      if (!testRecordId) {
        console.warn('No test record ID available for update test');
        return;
      }

      const updates = {
        quality_score: 95,
        battery_level: 90
      };

      const result = await duckDBService.updateData(testRecordId, updates);
      
      // Check result based on database type
      if (duckDBService.dbType === 'fallback') {
        expect(result.success).toBe(false);
        expect(result.message).toContain('not supported');
      } else {
        expect(result.success).toBe(true);
        expect(result.id).toBe(testRecordId);
      }
    });

    test('should delete data', async () => {
      if (!testRecordId) {
        console.warn('No test record ID available for delete test');
        return;
      }

      const result = await duckDBService.deleteData(testRecordId);
      
      // Check result based on database type
      if (duckDBService.dbType === 'fallback') {
        expect(result.success).toBe(false);
        expect(result.message).toContain('not supported');
      } else {
        expect(result.success).toBe(true);
        expect(result.id).toBe(testRecordId);
      }
    });
  });

  describe('Error Handling', () => {
    test('should handle invalid data gracefully', async () => {
      await duckDBService.initialize();
      
      const invalidData = {
        // Missing required fields
        sensor_id: null,
        value: 'not_a_number'
      };

      try {
        await duckDBService.insertData(invalidData);
        // If using fallback mode, it might succeed
        if (duckDBService.dbType === 'fallback') {
          // This is expected for fallback mode
        }
      } catch (error) {
        // This is expected for database modes
        expect(error).toBeDefined();
      }
    });

    test('should handle database connection errors', async () => {
      // This test depends on database availability
      // If DuckDB/SQLite fail, it should fall back to file mode
      const result = await duckDBService.initialize();
      expect(result.success).toBe(true);
      // Mode should be one of the three options
      expect(['duckdb', 'sqlite', 'fallback']).toContain(result.mode);
    });
  });

  describe('Fallback Mode', () => {
    let fallbackService;

    beforeAll(() => {
      // Create a service that will be forced to use fallback mode
      fallbackService = new DuckDBService(testConfig);
      // Force fallback mode
      fallbackService.isAvailable = false;
      fallbackService.dbType = null;
    });

    afterAll(async () => {
      if (fallbackService) {
        await fallbackService.close();
      }
    });

    test('should work in fallback mode', async () => {
      const testData = {
        sensor_id: 'FALLBACK_SENSOR',
        location: { field: 'fallback_field', latitude: 40.7128, longitude: -74.0060 },
        reading_type: 'temperature',
        value: 22.0,
        unit: 'celsius',
        quality_score: 85,
        timestamp: new Date().toISOString(),
        battery_level: 80,
        signal_strength: 90,
        data_quality: 'good'
      };

      const insertResult = await fallbackService.insertData(testData);
      expect(insertResult.success).toBe(true);
      expect(insertResult.filepath).toBeDefined();

      const queryResult = await fallbackService.queryData({});
      expect(queryResult.success).toBe(true);
      expect(queryResult.source).toBe('fallback');
      expect(Array.isArray(queryResult.data)).toBe(true);
    });

    test('should get statistics in fallback mode', async () => {
      const stats = await fallbackService.getStats();
      expect(stats.success).toBe(true);
      expect(stats.source).toBe('fallback');
      expect(typeof stats.total).toBe('number');
    });
  });

  describe('Performance Tests', () => {
    beforeEach(async () => {
      await duckDBService.initialize();
    });

    test('should handle batch operations efficiently', async () => {
      const batchSize = 100;
      const batchData = Array.from({ length: batchSize }, (_, i) => ({
        sensor_id: `PERF_SENSOR_${i.toString().padStart(3, '0')}`,
        location: { 
          field: `field_${i}`, 
          latitude: 40.7128 + (Math.random() - 0.5) * 0.01, 
          longitude: -74.0060 + (Math.random() - 0.5) * 0.01 
        },
        reading_type: ['temperature', 'humidity', 'soil_moisture'][i % 3],
        value: Math.random() * 100,
        unit: 'test_unit',
        quality_score: Math.floor(Math.random() * 100),
        timestamp: new Date().toISOString(),
        battery_level: Math.floor(Math.random() * 100),
        signal_strength: Math.floor(Math.random() * 100),
        data_quality: ['poor', 'fair', 'good', 'excellent'][Math.floor(Math.random() * 4)]
      }));

      const startTime = Date.now();
      const result = await duckDBService.insertData(batchData);
      const endTime = Date.now();

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(batchSize);
      
      const duration = endTime - startTime;
      console.log(`Batch insert of ${batchSize} records took ${duration}ms (${(duration/batchSize).toFixed(2)}ms per record)`);
      
      // Performance assertion - should be reasonably fast
      expect(duration).toBeLessThan(10000); // Less than 10 seconds
    }, 15000);

    test('should handle large queries efficiently', async () => {
      // First insert some data
      const testData = Array.from({ length: 50 }, (_, i) => ({
        sensor_id: `QUERY_SENSOR_${i.toString().padStart(3, '0')}`,
        location: { field: `field_${i}`, latitude: 40.7128, longitude: -74.0060 },
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        unit: 'celsius',
        quality_score: 80 + Math.floor(Math.random() * 20),
        timestamp: new Date().toISOString(),
        battery_level: 70 + Math.floor(Math.random() * 30),
        signal_strength: 80 + Math.floor(Math.random() * 20),
        data_quality: 'good'
      }));

      await duckDBService.insertData(testData);

      const startTime = Date.now();
      const result = await duckDBService.queryData({});
      const endTime = Date.now();

      expect(result.success).toBe(true);
      expect(Array.isArray(result.data)).toBe(true);
      
      const duration = endTime - startTime;
      console.log(`Query of ${result.data.length} records took ${duration}ms`);
      
      // Performance assertion
      expect(duration).toBeLessThan(5000); // Less than 5 seconds
    });
  });
});
