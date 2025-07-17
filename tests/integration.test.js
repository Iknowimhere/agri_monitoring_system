const request = require('supertest');
const { app, initializeDatabase } = require('../src/app');

// Apply global mocks for test isolation
global.testMode = true;
const mockPath = require('path');
global.testId = `test_${Date.now()}`;
global.mockDbPath = mockPath.join(__dirname, '..', 'data', 'database', `${global.testId}.duckdb`);

const DuckDBService = require('../src/services/duckDBService');
const DataService = require('../src/services/dataService');
const PipelineService = require('../src/services/pipelineService');
const ReportsService = require('../src/services/reportsService');

describe('Complete DuckDB Integration Tests', () => {
  let server;
  let duckDBService;
  let sharedDuckDBService;

  beforeAll(async () => {
    // Apply test mode configuration
    process.env.NODE_ENV = 'test';
    
    // Create a shared DuckDB service instance for all services to use
    sharedDuckDBService = new DuckDBService({ testId: global.testId });
    await sharedDuckDBService.initialize();
    
    // Set up global mock to use the shared instance
    global.mockDuckDBService = sharedDuckDBService;
    
    // Initialize the database before starting tests
    await initializeDatabase();
    
    // Start server for API testing
    server = app.listen(0); // Use port 0 for random available port
    
    // Use the shared service instance
    duckDBService = sharedDuckDBService;
  });

  afterAll(async () => {
    // Clean up
    if (sharedDuckDBService) {
      await sharedDuckDBService.close();
    }
    if (server) {
      server.close();
    }
    
    // Clear global mock
    global.mockDuckDBService = null;
  });

  beforeEach(async () => {
    // Clear any existing test data
    try {
      await duckDBService.queryData({ limit: 1000 });
    } catch (error) {
      // Ignore errors - database might be empty
    }
  });

  describe('End-to-End Data Flow', () => {
    test('should handle complete data pipeline: ingest → process → store → query → report', async () => {
      // Step 1: Ingest sample data through API
      const sampleData = [
        {
          sensor_id: 'E2E_SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: new Date().toISOString(),
          field: 'test_field_e2e'
        },
        {
          sensor_id: 'E2E_SENSOR_002',
          reading_type: 'humidity',
          value: 65.0,
          timestamp: new Date().toISOString(),
          field: 'test_field_e2e'
        },
        {
          sensor_id: 'E2E_SENSOR_001',
          reading_type: 'soil_moisture',
          value: 45.0,
          timestamp: new Date().toISOString(),
          field: 'test_field_e2e'
        }
      ];

      // Step 2: Process data through pipeline
      const pipelineResult = await PipelineService.processData(sampleData);
      expect(pipelineResult.success).toBe(true);
      expect(pipelineResult.processed).toBe(3);
      expect(pipelineResult.stored).toBe(3);
      expect(pipelineResult.duckdb.initialized).toBe(true);

      // Step 3: Query data through API
      const queryResponse = await request(server)
        .get('/api/data/query')
        .query({ 
          sensor_id: 'E2E_SENSOR_001',
          field: 'test_field_e2e'
        })
        .expect(200);

      expect(queryResponse.body.data).toBeDefined();
      expect(queryResponse.body.data.length).toBeGreaterThan(0);
      expect(queryResponse.body.pagination).toBeDefined();

      // Step 4: Get sensor summary
      const sensorResponse = await request(server)
        .get('/api/data/sensors/E2E_SENSOR_001/summary')
        .expect(200);

      expect(sensorResponse.body.sensor_id).toBe('E2E_SENSOR_001');
      expect(sensorResponse.body.total_readings).toBe(2);
      expect(sensorResponse.body.reading_types).toContain('temperature');
      expect(sensorResponse.body.reading_types).toContain('soil_moisture');

      // Step 5: Generate analytics report
      const analyticsResponse = await request(server)
        .post('/api/reports/analytics')
        .send({
          sensor_id: 'E2E_SENSOR_001',
          field: 'test_field_e2e'
        })
        .expect(200);

      expect(analyticsResponse.body.success).toBe(true);
      expect(analyticsResponse.body.report.metadata.source).toBe('duckdb');
      expect(analyticsResponse.body.report.statistics).toBeDefined();

      // Step 6: Generate quality report
      const qualityResponse = await request(server)
        .post('/api/reports/quality')
        .send({
          field: 'test_field_e2e'
        })
        .expect(200);

      expect(qualityResponse.body.success).toBe(true);
      expect(qualityResponse.body.report.metadata.source).toBe('duckdb');
      expect(qualityResponse.body.report.quality_metrics).toBeDefined();
    });

    test('should handle data updates and maintain consistency', async () => {
      // Insert initial data
      const initialData = {
        sensor_id: 'UPDATE_TEST_SENSOR',
        reading_type: 'temperature',
        value: 20.0,
        timestamp: new Date().toISOString(),
        field: 'update_test_field'
      };

      const insertResult = await DataService.insertData(initialData);
      expect(insertResult.success).toBe(true);
      const recordId = insertResult.id;

      // Query initial data
      let queryResult = await DataService.queryData({ 
        sensor_id: 'UPDATE_TEST_SENSOR' 
      });
      expect(queryResult.data.length).toBe(1);
      expect(queryResult.data[0].value).toBe(20.0);

      // Update the data
      const updateResult = await DataService.updateData(recordId, { 
        value: 25.0 
      });
      expect(updateResult.success).toBe(true);

      // Verify update
      queryResult = await DataService.queryData({ 
        sensor_id: 'UPDATE_TEST_SENSOR' 
      });
      expect(queryResult.data.length).toBe(1);
      expect(queryResult.data[0].value).toBe(25.0);

      // Delete the data
      const deleteResult = await DataService.deleteData(recordId);
      expect(deleteResult.success).toBe(true);

      // Verify deletion
      queryResult = await DataService.queryData({ 
        sensor_id: 'UPDATE_TEST_SENSOR' 
      });
      expect(queryResult.data.length).toBe(0);
    });
  });

  describe('Performance Testing', () => {
    test('should handle large dataset operations efficiently', async () => {
      const startTime = Date.now();
      
      // Generate large dataset
      const largeDataset = Array.from({ length: 1000 }, (_, i) => ({
        sensor_id: `PERF_SENSOR_${String(i % 10).padStart(2, '0')}`,
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        timestamp: new Date(Date.now() - i * 60000).toISOString(),
        field: 'performance_test'
      }));

      // Process through pipeline
      const pipelineResult = await PipelineService.processData(largeDataset);
      expect(pipelineResult.success).toBe(true);
      expect(pipelineResult.processed).toBe(1000);

      const pipelineTime = Date.now();
      
      // Query aggregated data
      const aggregationResult = await DataService.getDailyAggregations({
        field: 'performance_test',
        startDate: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        endDate: new Date().toISOString()
      });
      
      expect(aggregationResult.success).toBe(true);
      expect(aggregationResult.source).toBe('duckdb');

      const queryTime = Date.now();

      // Generate analytics report
      const reportResult = await ReportsService.generateAnalyticsReport({
        field: 'performance_test'
      });
      
      expect(reportResult.metadata.source).toBe('duckdb');
      expect(reportResult.statistics.count).toBeGreaterThan(0);

      const reportTime = Date.now();

      // Performance assertions
      const totalTime = reportTime - startTime;
      const pipelineDuration = pipelineTime - startTime;
      const queryDuration = queryTime - pipelineTime;
      const reportDuration = reportTime - queryTime;

      console.log(`Performance Test Results:
        - Pipeline (1000 records): ${pipelineDuration}ms
        - Aggregation Query: ${queryDuration}ms  
        - Analytics Report: ${reportDuration}ms
        - Total Time: ${totalTime}ms`);

      // Performance targets
      expect(pipelineDuration).toBeLessThan(5000); // 5 seconds for pipeline
      expect(queryDuration).toBeLessThan(1000);    // 1 second for queries
      expect(reportDuration).toBeLessThan(3000);   // 3 seconds for reports
      expect(totalTime).toBeLessThan(10000);       // 10 seconds total
    });

    test('should handle concurrent operations', async () => {
      const concurrentPromises = [];
      
      // Create multiple concurrent operations
      for (let i = 0; i < 5; i++) {
        const data = {
          sensor_id: `CONCURRENT_SENSOR_${i}`,
          reading_type: 'temperature',
          value: 20 + i,
          timestamp: new Date().toISOString(),
          field: 'concurrent_test'
        };
        
        concurrentPromises.push(DataService.insertData(data));
      }

      // Wait for all operations to complete
      const results = await Promise.all(concurrentPromises);
      
      // Verify all operations succeeded
      results.forEach(result => {
        expect(result.success).toBe(true);
      });

      // Verify data integrity
      const queryResult = await DataService.queryData({
        field: 'concurrent_test'
      });
      
      expect(queryResult.data.length).toBe(5);
      expect(queryResult.source).toBe('duckdb');
    });
  });

  describe('Error Handling and Recovery', () => {
    test('should handle malformed data gracefully', async () => {
      const malformedData = [
        {
          sensor_id: 'MALFORMED_SENSOR',
          reading_type: 'temperature',
          value: 'not_a_number', // Invalid value
          timestamp: new Date().toISOString(),
          field: 'error_test'
        },
        {
          // Missing required fields
          value: 25.0
        },
        {
          sensor_id: 'VALID_SENSOR',
          reading_type: 'temperature',
          value: 25.0,
          timestamp: new Date().toISOString(),
          field: 'error_test'
        }
      ];

      const result = await PipelineService.processData(malformedData);
      
      expect(result.success).toBe(true);
      expect(result.validation.passed).toBe(1); // Only one valid record
      expect(result.validation.failed).toBe(2); // Two invalid records
      expect(result.validation.errors.length).toBe(2);
    });

    test('should maintain data consistency during errors', async () => {
      // Insert valid data first
      const validData = {
        sensor_id: 'CONSISTENCY_SENSOR',
        reading_type: 'temperature',
        value: 25.0,
        timestamp: new Date().toISOString(),
        field: 'consistency_test'
      };

      await DataService.insertData(validData);

      // Verify data exists
      let queryResult = await DataService.queryData({
        sensor_id: 'CONSISTENCY_SENSOR'
      });
      expect(queryResult.data.length).toBe(1);

      // Attempt to insert invalid data
      const invalidData = {
        sensor_id: 'CONSISTENCY_SENSOR',
        reading_type: 'temperature',
        value: null, // Invalid value
        timestamp: new Date().toISOString(),
        field: 'consistency_test'
      };

      try {
        await DataService.insertData(invalidData);
      } catch (error) {
        // Expected to fail
      }

      // Verify original data is still intact
      queryResult = await DataService.queryData({
        sensor_id: 'CONSISTENCY_SENSOR'
      });
      expect(queryResult.data.length).toBe(1);
      expect(queryResult.data[0].value).toBe(25.0);
    });
  });

  describe('API Integration', () => {
    test('should provide consistent API responses with DuckDB backend', async () => {
      // Test health endpoint
      const healthResponse = await request(server)
        .get('/health')
        .expect(200);

      expect(healthResponse.body.status).toBe('healthy');
      expect(healthResponse.body.database).toBeDefined();
      expect(healthResponse.body.database.duckdb).toBeDefined();

      // Test data endpoint
      const dataResponse = await request(server)
        .get('/api/data')
        .query({ limit: 5 })
        .expect(200);

      expect(dataResponse.body.success).toBe(true);
      expect(dataResponse.body.source).toBe('duckdb');
      expect(Array.isArray(dataResponse.body.data)).toBe(true);

      // Test sensors endpoint
      const sensorsResponse = await request(server)
        .get('/api/data/sensors')
        .expect(200);

      expect(Array.isArray(sensorsResponse.body)).toBe(true);

      // Test pipeline status
      const pipelineResponse = await request(server)
        .get('/api/pipeline/status')
        .expect(200);

      expect(pipelineResponse.body.duckdb).toBeDefined();
      expect(pipelineResponse.body.duckdb.status).toBeDefined();
    });

    test('should handle API errors gracefully', async () => {
      // Test invalid sensor ID
      const invalidSensorResponse = await request(server)
        .get('/api/data/sensors/INVALID_SENSOR_ID/summary')
        .expect(200); // Should return empty summary, not error

      expect(invalidSensorResponse.body.sensor_id).toBe('INVALID_SENSOR_ID');
      expect(invalidSensorResponse.body.total_readings).toBe(0);

      // Test invalid date format
      const invalidDateResponse = await request(server)
        .get('/api/data')
        .query({ 
          startDate: 'invalid-date-format'
        })
        .expect(400); // Should return validation error

      expect(invalidDateResponse.body.success).toBe(false);
      expect(invalidDateResponse.body.error).toBeDefined();
    });
  });

  describe('Data Quality and Validation', () => {
    test('should maintain data quality standards', async () => {
      // Insert test data with quality variations
      const testData = [
        {
          sensor_id: 'QUALITY_SENSOR_001',
          reading_type: 'temperature',
          value: 25.0,
          timestamp: new Date().toISOString(),
          field: 'quality_test'
        },
        {
          sensor_id: 'QUALITY_SENSOR_001',
          reading_type: 'temperature',
          value: 25.1,
          timestamp: new Date(Date.now() + 60000).toISOString(),
          field: 'quality_test'
        },
        {
          sensor_id: 'QUALITY_SENSOR_001',
          reading_type: 'temperature',
          value: 100.0, // Potential outlier
          timestamp: new Date(Date.now() + 120000).toISOString(),
          field: 'quality_test'
        }
      ];

      const pipelineResult = await PipelineService.processData(testData);
      expect(pipelineResult.success).toBe(true);

      // Generate quality report
      const qualityReport = await ReportsService.generateQualityReport({
        sensor_id: 'QUALITY_SENSOR_001',
        field: 'quality_test'
      });

      expect(qualityReport.metadata.source).toBe('duckdb');
      expect(qualityReport.quality_metrics.total_records).toBe(3);
      expect(qualityReport.quality_metrics.completeness_rate).toBe(1.0);
      expect(qualityReport.quality_metrics.outliers).toBeGreaterThan(0);
    });
  });

  describe('System Integration Health', () => {
    test('should verify all services are properly integrated with DuckDB', async () => {
      // Test DataService integration
      const dataStats = await DataService.getStats();
      expect(dataStats.source).toBe('duckdb');

      // Test PipelineService integration
      const pipelineStatus = await PipelineService.getStatus();
      expect(pipelineStatus.duckdb).toBeDefined();
      expect(pipelineStatus.duckdb.initialized).toBe(true);

      // Test ReportsService integration
      const summaryReport = await ReportsService.generateSummaryReport({});
      expect(summaryReport.metadata.source).toBe('duckdb');

      // Verify database connection is healthy
      const testQuery = await duckDBService.queryData({ limit: 1 });
      expect(testQuery.success).toBe(true);
      expect(testQuery.source).toBe('duckdb');
    });

    test('should handle service lifecycle properly', async () => {
      // Test service initialization
      await DataService.ensureInitialized();
      await PipelineService.processData([]); // Should not fail

      // Services should be operational
      const dataResult = await DataService.queryData({ limit: 1 });
      expect(dataResult.success).toBe(true);

      const reportResult = await ReportsService.generateSummaryReport({});
      expect(reportResult.metadata).toBeDefined();
    });
  });
});
