const DataService = require('../src/services/dataService');
const DuckDBService = require('../src/services/duckDBService');

// Mock DuckDBService for testing
jest.mock('../src/services/duckDBService');

describe('DataService with DuckDB Integration', () => {
  let mockDuckDBService;

  beforeEach(() => {
    // Clear all mocks
    jest.clearAllMocks();
    
    // Create mock DuckDB service
    mockDuckDBService = {
      initialize: jest.fn().mockResolvedValue({ success: true, mode: 'duckdb' }),
      insertData: jest.fn().mockResolvedValue({ success: true, recordCount: 1 }),
      queryData: jest.fn().mockResolvedValue({ 
        success: true, 
        data: [
          {
            id: 1,
            sensor_id: 'TEST_SENSOR_001',
            reading_type: 'temperature',
            value: 25.5,
            timestamp: new Date().toISOString(),
            field: 'test_field'
          }
        ], 
        source: 'duckdb' 
      }),
      updateData: jest.fn().mockResolvedValue({ success: true, updated: 1, id: 1 }),
      deleteData: jest.fn().mockResolvedValue({ success: true, deleted: 1, id: 1 }),
      getStats: jest.fn().mockResolvedValue({
        success: true,
        source: 'duckdb',
        total: 100,
        byType: [
          { reading_type: 'temperature', count: 40 },
          { reading_type: 'humidity', count: 35 },
          { reading_type: 'soil_moisture', count: 25 }
        ],
        latestTimestamp: new Date().toISOString()
      }),
      close: jest.fn().mockResolvedValue()
    };

    // Mock the DuckDBService constructor
    DuckDBService.mockImplementation(() => mockDuckDBService);
    
    // Set global mocks for the DataService to use
    global.mockDuckDBService = mockDuckDBService;
    
    // Reset DataService initialization status to force re-initialization with mocks
    DataService.initialized = false;
    DataService.duckDBService = null;
  });

  afterEach(() => {
    // Clean up global mocks
    delete global.mockDuckDBService;
  });

  describe('Initialization', () => {
    test('should initialize DataService with DuckDB', async () => {
      expect(DataService).toBeDefined();
      
      // Initialize the service
      await DataService.ensureInitialized();
      
      // In test mode, we use global mocks, so the constructor isn't called
      // but the initialize method on the mock should be called
      expect(mockDuckDBService.initialize).toHaveBeenCalled();
    });
  });

  describe('Data Querying', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should query data with basic filters', async () => {
      const filters = {
        sensor_id: 'TEST_SENSOR_001',
        page: 1,
        limit: 10
      };

      const result = await DataService.queryData(filters);

      expect(result).toBeDefined();
      expect(result.data).toBeDefined();
      expect(Array.isArray(result.data)).toBe(true);
      expect(result.source).toBe('duckdb');
      expect(result.page).toBe(1);
      expect(result.limit).toBe(10);
      
      expect(mockDuckDBService.queryData).toHaveBeenCalledWith({
        sensor_id: 'TEST_SENSOR_001',
        reading_type: undefined,
        startDate: undefined,
        endDate: undefined,
        field: undefined,
        limit: 10
      });
    });

    test('should handle pagination correctly', async () => {
      const filters = {
        page: 2,
        limit: 5
      };

      const result = await DataService.queryData(filters);

      expect(result.page).toBe(2);
      expect(result.limit).toBe(5);
    });

    test('should apply additional filters not handled by DuckDB', async () => {
      // Mock data with anomalous reading
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            id: 1, 
            sensor_id: 'TEST_SENSOR_001', 
            value: 25.5, 
            anomalous_reading: true 
          },
          { 
            id: 2, 
            sensor_id: 'TEST_SENSOR_002', 
            value: 26.0, 
            anomalous_reading: false 
          }
        ],
        source: 'duckdb'
      });

      const filters = {
        anomalous: true
      };

      const result = await DataService.queryData(filters);

      expect(result.data).toHaveLength(1);
      expect(result.data[0].anomalous_reading).toBe(true);
    });

    test('should apply value range filters', async () => {
      // Mock data with different values
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { id: 1, sensor_id: 'TEST_SENSOR_001', value: 15.0 },
          { id: 2, sensor_id: 'TEST_SENSOR_002', value: 25.0 },
          { id: 3, sensor_id: 'TEST_SENSOR_003', value: 35.0 }
        ],
        source: 'duckdb'
      });

      const filters = {
        minValue: 20.0,
        maxValue: 30.0
      };

      const result = await DataService.queryData(filters);

      expect(result.data).toHaveLength(1);
      expect(result.data[0].value).toBe(25.0);
    });
  });

  describe('Sensor Operations', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should get unique sensors', async () => {
      // Mock sensor data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'temperature', 
            field: 'field_1',
            timestamp: '2025-07-16T10:00:00Z'
          },
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'humidity', 
            field: 'field_1',
            timestamp: '2025-07-16T11:00:00Z'
          },
          { 
            sensor_id: 'SENSOR_002', 
            reading_type: 'temperature', 
            field: 'field_2',
            timestamp: '2025-07-16T10:30:00Z'
          }
        ],
        source: 'duckdb'
      });

      const sensors = await DataService.getSensors();

      expect(sensors).toBeDefined();
      expect(Array.isArray(sensors)).toBe(true);
      expect(sensors).toHaveLength(2);
      
      const sensor1 = sensors.find(s => s.sensor_id === 'SENSOR_001');
      expect(sensor1).toBeDefined();
      expect(sensor1.reading_types).toContain('temperature');
      expect(sensor1.reading_types).toContain('humidity');
      expect(sensor1.total_readings).toBe(2);
    });

    test('should get sensor summary', async () => {
      // Mock sensor summary data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'temperature', 
            value: 25.0,
            timestamp: '2025-07-16T10:00:00Z'
          },
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'temperature', 
            value: 26.0,
            timestamp: '2025-07-16T11:00:00Z'
          }
        ],
        source: 'duckdb'
      });

      const summary = await DataService.getSensorSummary('SENSOR_001');

      expect(summary).toBeDefined();
      expect(summary.sensor_id).toBe('SENSOR_001');
      expect(summary.total_readings).toBe(2);
      expect(summary.reading_types).toContain('temperature');
      expect(summary.statistics.temperature).toBeDefined();
      expect(summary.statistics.temperature.min).toBe(25.0);
      expect(summary.statistics.temperature.max).toBe(26.0);
      expect(summary.statistics.temperature.avg).toBe(25.5);
    });

    test('should handle empty sensor summary', async () => {
      // Mock empty data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [],
        source: 'duckdb'
      });

      const summary = await DataService.getSensorSummary('NON_EXISTENT_SENSOR');

      expect(summary).toBeDefined();
      expect(summary.sensor_id).toBe('NON_EXISTENT_SENSOR');
      expect(summary.total_readings).toBe(0);
      expect(summary.reading_types).toHaveLength(0);
      expect(summary.date_range).toBeNull();
    });
  });

  describe('Aggregations', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should get daily aggregations', async () => {
      // Mock daily data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            sensor_id: 'SENSOR_001', 
            value: 25.0,
            timestamp: '2025-07-16T10:00:00Z'
          },
          { 
            sensor_id: 'SENSOR_001', 
            value: 26.0,
            timestamp: '2025-07-16T14:00:00Z'
          },
          { 
            sensor_id: 'SENSOR_001', 
            value: 24.0,
            timestamp: '2025-07-17T10:00:00Z'
          }
        ],
        source: 'duckdb'
      });

      const aggregations = await DataService.getDailyAggregations({
        sensor_id: 'SENSOR_001',
        startDate: '2025-07-16',
        endDate: '2025-07-17'
      });

      expect(aggregations).toBeDefined();
      expect(aggregations.aggregations).toBeDefined();
      expect(Array.isArray(aggregations.aggregations)).toBe(true);
      expect(aggregations.total_days).toBeGreaterThan(0);
      expect(aggregations.source).toBe('duckdb');

      // Check if aggregations are properly calculated
      const day1 = aggregations.aggregations.find(a => a.date === '2025-07-16');
      if (day1) {
        expect(day1.count).toBe(2);
        expect(day1.min).toBe(25.0);
        expect(day1.max).toBe(26.0);
        expect(day1.avg).toBe(25.5);
      }
    });
  });

  describe('Data Export', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should export data as CSV', async () => {
      // Mock export data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'temperature',
            value: 25.0,
            timestamp: '2025-07-16T10:00:00Z'
          }
        ],
        source: 'duckdb'
      });

      const csvData = await DataService.exportData('csv', { sensor_id: 'SENSOR_001' });

      expect(csvData).toBeDefined();
      expect(typeof csvData).toBe('string');
      expect(csvData).toContain('sensor_id');
      expect(csvData).toContain('SENSOR_001');
      expect(csvData).toContain('temperature');
    });

    test('should export data as JSON', async () => {
      // Mock export data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { 
            sensor_id: 'SENSOR_001', 
            reading_type: 'temperature',
            value: 25.0
          }
        ],
        source: 'duckdb'
      });

      const jsonData = await DataService.exportData('json', { sensor_id: 'SENSOR_001' });

      expect(jsonData).toBeDefined();
      expect(typeof jsonData).toBe('string');
      
      const parsedData = JSON.parse(jsonData);
      expect(Array.isArray(parsedData)).toBe(true);
      expect(parsedData[0].sensor_id).toBe('SENSOR_001');
    });

    test('should throw error for unsupported export format', async () => {
      await expect(
        DataService.exportData('xml', {})
      ).rejects.toThrow('Unsupported export format: xml');
    });
  });

  describe('CRUD Operations', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should insert data', async () => {
      const testData = {
        sensor_id: 'TEST_SENSOR',
        reading_type: 'temperature',
        value: 25.0
      };

      const result = await DataService.insertData(testData);

      expect(result).toBeDefined();
      expect(mockDuckDBService.insertData).toHaveBeenCalledWith(testData);
    });

    test('should update data', async () => {
      const updates = { value: 26.0 };

      const result = await DataService.updateData(1, updates);

      expect(result).toBeDefined();
      expect(mockDuckDBService.updateData).toHaveBeenCalledWith(1, updates);
    });

    test('should delete data', async () => {
      const result = await DataService.deleteData(1);

      expect(result).toBeDefined();
      expect(mockDuckDBService.deleteData).toHaveBeenCalledWith(1);
    });

    test('should get statistics', async () => {
      const stats = await DataService.getStats();

      expect(stats).toBeDefined();
      expect(stats.total).toBe(100);
      expect(stats.source).toBe('duckdb');
      expect(Array.isArray(stats.byType)).toBe(true);
      expect(mockDuckDBService.getStats).toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      await DataService.ensureInitialized();
    });

    test('should handle DuckDB query errors', async () => {
      mockDuckDBService.queryData.mockRejectedValueOnce(new Error('Database connection failed'));

      await expect(DataService.queryData({})).rejects.toThrow('Database connection failed');
    });

    test('should handle DuckDB insert errors', async () => {
      mockDuckDBService.insertData.mockRejectedValueOnce(new Error('Insert failed'));

      await expect(DataService.insertData({})).rejects.toThrow('Insert failed');
    });

    test('should handle initialization errors gracefully', async () => {
      // Reset the service to force re-initialization
      DataService.initialized = false;
      DataService.duckDBService = null;
      
      mockDuckDBService.initialize.mockRejectedValueOnce(new Error('Init failed'));

      // Should reject with the initialization error
      await expect(DataService.ensureInitialized()).rejects.toThrow('Init failed');
    });
  });

  describe('Service Lifecycle', () => {
    test('should close service properly', async () => {
      // Initialize the service first
      await DataService.ensureInitialized();
      
      // Then close it
      await DataService.close();
      expect(mockDuckDBService.close).toHaveBeenCalled();
    });
  });
});
