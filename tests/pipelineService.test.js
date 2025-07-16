const PipelineService = require('../src/services/pipelineService');
const DuckDBService = require('../src/services/duckDBService');
const StorageService = require('../src/services/storage/storageService');

// Mock dependencies
jest.mock('../src/services/duckDBService');
jest.mock('../src/services/storage/storageService');

describe('PipelineService with DuckDB Integration', () => {
  let mockDuckDBService;
  let mockStorageService;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Mock DuckDB service
    mockDuckDBService = {
      initialize: jest.fn().mockResolvedValue({ success: true, mode: 'duckdb' }),
      insertData: jest.fn().mockResolvedValue({ success: true, recordCount: 1 }),
      queryData: jest.fn().mockResolvedValue({ 
        success: true, 
        data: [], 
        source: 'duckdb' 
      }),
      close: jest.fn().mockResolvedValue()
    };

    // Mock Storage service
    mockStorageService = {
      storeProcessedData: jest.fn().mockResolvedValue({ 
        success: true, 
        filePath: 'test-processed.json' 
      }),
      storeValidationReport: jest.fn().mockResolvedValue({ 
        success: true, 
        filePath: 'test-validation.json' 
      }),
      storeRawData: jest.fn().mockResolvedValue({ 
        success: true, 
        filePath: 'test-raw.json' 
      })
    };

    DuckDBService.mockImplementation(() => mockDuckDBService);
    StorageService.storeProcessedData = mockStorageService.storeProcessedData;
    StorageService.storeValidationReport = mockStorageService.storeValidationReport;
    StorageService.storeRawData = mockStorageService.storeRawData;
  });

  describe('Pipeline Processing', () => {
    test('should process valid sensor data through complete pipeline', async () => {
      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        },
        {
          sensor_id: 'SENSOR_002',
          reading_type: 'humidity',
          value: 65.0,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        }
      ];

      const result = await PipelineService.processData(inputData);

      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.processed).toBeGreaterThan(0);
      expect(result.stored).toBeGreaterThan(0);
      expect(result.validation).toBeDefined();
      expect(result.storage).toBeDefined();
      expect(result.duckdb).toBeDefined();

      // Verify DuckDB operations
      expect(mockDuckDBService.initialize).toHaveBeenCalled();
      expect(mockDuckDBService.insertData).toHaveBeenCalled();

      // Verify storage operations
      expect(mockStorageService.storeProcessedData).toHaveBeenCalled();
      expect(mockStorageService.storeValidationReport).toHaveBeenCalled();
    });

    test('should handle data with validation errors', async () => {
      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 'invalid_value', // Invalid value
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        },
        {
          sensor_id: '', // Missing sensor_id
          reading_type: 'humidity',
          value: 65.0,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        }
      ];

      const result = await PipelineService.processData(inputData);

      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.validation.passed).toBe(0);
      expect(result.validation.failed).toBe(2);
      expect(result.validation.errors).toHaveLength(2);

      // Should still store validation report
      expect(mockStorageService.storeValidationReport).toHaveBeenCalled();
    });

    test('should transform data correctly', async () => {
      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1',
          extra_field: 'should_be_removed'
        }
      ];

      const result = await PipelineService.processData(inputData);

      expect(result.success).toBe(true);
      
      // Check that transformation occurred
      const transformedData = result.processed_data;
      expect(transformedData).toBeDefined();
      expect(Array.isArray(transformedData)).toBe(true);
      
      if (transformedData.length > 0) {
        const firstRecord = transformedData[0];
        expect(firstRecord.extra_field).toBeUndefined();
        expect(firstRecord.sensor_id).toBe('SENSOR_001');
        expect(firstRecord.processed_at).toBeDefined();
      }
    });

    test('should handle empty input data', async () => {
      const result = await PipelineService.processData([]);

      expect(result).toBeDefined();
      expect(result.success).toBe(true);
      expect(result.processed).toBe(0);
      expect(result.stored).toBe(0);
      expect(result.validation.passed).toBe(0);
      expect(result.validation.failed).toBe(0);
    });

    test('should handle null or undefined input', async () => {
      await expect(PipelineService.processData(null)).rejects.toThrow();
      await expect(PipelineService.processData(undefined)).rejects.toThrow();
    });
  });

  describe('Pipeline Status', () => {
    test('should return correct pipeline status', async () => {
      const status = await PipelineService.getStatus();

      expect(status).toBeDefined();
      expect(status.status).toBeDefined();
      expect(status.lastProcessed).toBeDefined();
      expect(status.totalProcessed).toBeDefined();
      expect(status.statistics).toBeDefined();
      expect(status.duckdb).toBeDefined();
    });

    test('should include DuckDB status in pipeline status', async () => {
      const status = await PipelineService.getStatus();

      expect(status.duckdb).toBeDefined();
      expect(status.duckdb.status).toBeDefined();
      expect(status.duckdb.initialized).toBeDefined();
    });
  });

  describe('Pipeline Configuration', () => {
    test('should get pipeline configuration', () => {
      const config = PipelineService.getConfig();

      expect(config).toBeDefined();
      expect(config.validation).toBeDefined();
      expect(config.storage).toBeDefined();
      expect(config.processing).toBeDefined();
      expect(config.duckdb).toBeDefined();
    });

    test('should update pipeline configuration', () => {
      const newConfig = {
        validation: {
          strictMode: true
        },
        storage: {
          enableBackup: false
        }
      };

      const result = PipelineService.updateConfig(newConfig);

      expect(result.success).toBe(true);
      
      const updatedConfig = PipelineService.getConfig();
      expect(updatedConfig.validation.strictMode).toBe(true);
      expect(updatedConfig.storage.enableBackup).toBe(false);
    });
  });

  describe('Data Validation', () => {
    test('should validate sensor data correctly', () => {
      const validData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: 25.5,
        timestamp: '2025-07-16T10:00:00Z',
        field: 'field_1'
      };

      const validation = PipelineService.validateRecord(validData);

      expect(validation.isValid).toBe(true);
      expect(validation.errors).toHaveLength(0);
    });

    test('should detect missing required fields', () => {
      const invalidData = {
        reading_type: 'temperature',
        value: 25.5
        // Missing sensor_id, timestamp, field
      };

      const validation = PipelineService.validateRecord(invalidData);

      expect(validation.isValid).toBe(false);
      expect(validation.errors.length).toBeGreaterThan(0);
      expect(validation.errors.some(error => error.includes('sensor_id'))).toBe(true);
    });

    test('should detect invalid data types', () => {
      const invalidData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: 'not_a_number',
        timestamp: '2025-07-16T10:00:00Z',
        field: 'field_1'
      };

      const validation = PipelineService.validateRecord(invalidData);

      expect(validation.isValid).toBe(false);
      expect(validation.errors.some(error => error.includes('value'))).toBe(true);
    });

    test('should detect invalid timestamps', () => {
      const invalidData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: 25.5,
        timestamp: 'invalid_timestamp',
        field: 'field_1'
      };

      const validation = PipelineService.validateRecord(invalidData);

      expect(validation.isValid).toBe(false);
      expect(validation.errors.some(error => error.includes('timestamp'))).toBe(true);
    });
  });

  describe('Data Transformation', () => {
    test('should transform data with standard fields', () => {
      const inputData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: 25.5,
        timestamp: '2025-07-16T10:00:00Z',
        field: 'field_1',
        extra_field: 'should_be_removed'
      };

      const transformed = PipelineService.transformRecord(inputData);

      expect(transformed.sensor_id).toBe('SENSOR_001');
      expect(transformed.reading_type).toBe('temperature');
      expect(transformed.value).toBe(25.5);
      expect(transformed.timestamp).toBe('2025-07-16T10:00:00Z');
      expect(transformed.field).toBe('field_1');
      expect(transformed.extra_field).toBeUndefined();
      expect(transformed.processed_at).toBeDefined();
    });

    test('should add processing metadata', () => {
      const inputData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: 25.5,
        timestamp: '2025-07-16T10:00:00Z',
        field: 'field_1'
      };

      const transformed = PipelineService.transformRecord(inputData);

      expect(transformed.processed_at).toBeDefined();
      expect(new Date(transformed.processed_at)).toBeInstanceOf(Date);
    });

    test('should handle numeric string values', () => {
      const inputData = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        value: '25.5',
        timestamp: '2025-07-16T10:00:00Z',
        field: 'field_1'
      };

      const transformed = PipelineService.transformRecord(inputData);

      expect(typeof transformed.value).toBe('number');
      expect(transformed.value).toBe(25.5);
    });
  });

  describe('Error Handling', () => {
    test('should handle DuckDB initialization failures', async () => {
      mockDuckDBService.initialize.mockRejectedValueOnce(new Error('DuckDB init failed'));

      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        }
      ];

      await expect(PipelineService.processData(inputData)).rejects.toThrow('DuckDB init failed');
    });

    test('should handle DuckDB insert failures', async () => {
      mockDuckDBService.insertData.mockRejectedValueOnce(new Error('Insert failed'));

      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        }
      ];

      await expect(PipelineService.processData(inputData)).rejects.toThrow('Insert failed');
    });

    test('should handle storage failures gracefully', async () => {
      mockStorageService.storeProcessedData.mockRejectedValueOnce(new Error('Storage failed'));

      const inputData = [
        {
          sensor_id: 'SENSOR_001',
          reading_type: 'temperature',
          value: 25.5,
          timestamp: '2025-07-16T10:00:00Z',
          field: 'field_1'
        }
      ];

      // Should continue processing even if storage fails
      const result = await PipelineService.processData(inputData);
      
      expect(result.success).toBe(true);
      expect(result.storage.success).toBe(false);
      expect(result.storage.error).toContain('Storage failed');
    });
  });

  describe('Performance', () => {
    test('should process large datasets efficiently', async () => {
      const largeDataset = Array.from({ length: 1000 }, (_, i) => ({
        sensor_id: `SENSOR_${String(i + 1).padStart(3, '0')}`,
        reading_type: 'temperature',
        value: 20 + Math.random() * 10,
        timestamp: new Date(Date.now() - i * 60000).toISOString(),
        field: 'test_field'
      }));

      const startTime = Date.now();
      const result = await PipelineService.processData(largeDataset);
      const endTime = Date.now();

      expect(result.success).toBe(true);
      expect(result.processed).toBe(1000);
      expect(endTime - startTime).toBeLessThan(5000); // Should complete in under 5 seconds
    });

    test('should handle batch processing', async () => {
      // Mock batch insert
      mockDuckDBService.insertData.mockResolvedValue({ 
        success: true, 
        recordCount: 100 
      });

      const batchData = Array.from({ length: 100 }, (_, i) => ({
        sensor_id: `SENSOR_${i}`,
        reading_type: 'temperature',
        value: 25.0,
        timestamp: new Date().toISOString(),
        field: 'test_field'
      }));

      const result = await PipelineService.processData(batchData);

      expect(result.success).toBe(true);
      expect(result.processed).toBe(100);
      expect(mockDuckDBService.insertData).toHaveBeenCalled();
    });
  });

  describe('Service Lifecycle', () => {
    test('should close pipeline service properly', async () => {
      await PipelineService.close();
      expect(mockDuckDBService.close).toHaveBeenCalled();
    });

    test('should reset pipeline state', () => {
      PipelineService.reset();
      
      const status = PipelineService.getStatus();
      expect(status.totalProcessed).toBe(0);
      expect(status.lastProcessed).toBeNull();
    });
  });
});
