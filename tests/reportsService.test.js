const ReportsService = require('../src/services/reportsService');
const DuckDBService = require('../src/services/duckDBService');

// Mock DuckDBService
jest.mock('../src/services/duckDBService');

describe('ReportsService with DuckDB Integration', () => {
  let mockDuckDBService;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Mock DuckDB service with analytical data
    mockDuckDBService = {
      initialize: jest.fn().mockResolvedValue({ success: true, mode: 'duckdb' }),
      queryData: jest.fn().mockResolvedValue({ 
        success: true, 
        data: [
          {
            sensor_id: 'SENSOR_001',
            reading_type: 'temperature',
            value: 25.5,
            timestamp: '2025-07-16T10:00:00Z',
            field: 'field_1'
          },
          {
            sensor_id: 'SENSOR_001',
            reading_type: 'temperature',
            value: 26.0,
            timestamp: '2025-07-16T11:00:00Z',
            field: 'field_1'
          },
          {
            sensor_id: 'SENSOR_002',
            reading_type: 'humidity',
            value: 65.0,
            timestamp: '2025-07-16T10:00:00Z',
            field: 'field_1'
          }
        ], 
        source: 'duckdb' 
      }),
      getStats: jest.fn().mockResolvedValue({
        success: true,
        source: 'duckdb',
        total: 150,
        byType: [
          { reading_type: 'temperature', count: 75 },
          { reading_type: 'humidity', count: 50 },
          { reading_type: 'soil_moisture', count: 25 }
        ],
        latestTimestamp: '2025-07-16T12:00:00Z'
      }),
      close: jest.fn().mockResolvedValue()
    };

    DuckDBService.mockImplementation(() => mockDuckDBService);
  });

  describe('Quality Reports', () => {
    test('should generate quality report with DuckDB data', async () => {
      const filters = {
        startDate: '2025-07-16T00:00:00Z',
        endDate: '2025-07-16T23:59:59Z',
        field: 'field_1'
      };

      const report = await ReportsService.generateQualityReport(filters);

      expect(report).toBeDefined();
      expect(report.metadata).toBeDefined();
      expect(report.metadata.source).toBe('duckdb');
      expect(report.summary).toBeDefined();
      expect(report.details).toBeDefined();
      expect(report.quality_metrics).toBeDefined();
      expect(report.recommendations).toBeDefined();

      // Verify DuckDB was called
      expect(mockDuckDBService.initialize).toHaveBeenCalled();
      expect(mockDuckDBService.queryData).toHaveBeenCalled();
    });

    test('should calculate correct quality metrics', async () => {
      // Mock data with quality issues
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { sensor_id: 'SENSOR_001', value: 25.5, timestamp: '2025-07-16T10:00:00Z' },
          { sensor_id: 'SENSOR_001', value: null, timestamp: '2025-07-16T11:00:00Z' }, // Missing value
          { sensor_id: 'SENSOR_001', value: 150.0, timestamp: '2025-07-16T12:00:00Z' }, // Outlier
          { sensor_id: 'SENSOR_002', value: 65.0, timestamp: '2025-07-16T10:00:00Z' }
        ],
        source: 'duckdb'
      });

      const report = await ReportsService.generateQualityReport({});

      expect(report.quality_metrics.total_records).toBe(4);
      expect(report.quality_metrics.missing_values).toBe(1);
      expect(report.quality_metrics.outliers).toBeGreaterThan(0);
      expect(report.quality_metrics.completeness_rate).toBeLessThan(1.0);
      expect(report.quality_metrics.accuracy_score).toBeLessThan(1.0);
    });

    test('should identify data quality issues', async () => {
      // Mock problematic data
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { sensor_id: 'SENSOR_001', value: null, reading_type: 'temperature' },
          { sensor_id: 'SENSOR_002', value: 999, reading_type: 'temperature' }, // Extreme outlier
          { sensor_id: '', value: 25.0, reading_type: 'temperature' }, // Missing sensor_id
        ],
        source: 'duckdb'
      });

      const report = await ReportsService.generateQualityReport({});

      expect(report.details.issues).toBeDefined();
      expect(Array.isArray(report.details.issues)).toBe(true);
      expect(report.details.issues.length).toBeGreaterThan(0);
      
      // Should identify different types of issues
      const issueTypes = report.details.issues.map(issue => issue.type);
      expect(issueTypes).toContain('missing_value');
    });

    test('should provide quality recommendations', async () => {
      const report = await ReportsService.generateQualityReport({});

      expect(report.recommendations).toBeDefined();
      expect(Array.isArray(report.recommendations)).toBe(true);
      expect(report.recommendations.length).toBeGreaterThan(0);
      
      // Check recommendation structure
      if (report.recommendations.length > 0) {
        const recommendation = report.recommendations[0];
        expect(recommendation.category).toBeDefined();
        expect(recommendation.priority).toBeDefined();
        expect(recommendation.description).toBeDefined();
        expect(recommendation.action).toBeDefined();
      }
    });
  });

  describe('Analytics Reports', () => {
    test('should generate analytics report with sensor statistics', async () => {
      const filters = {
        sensor_id: 'SENSOR_001',
        reading_type: 'temperature',
        startDate: '2025-07-16T00:00:00Z',
        endDate: '2025-07-16T23:59:59Z'
      };

      const report = await ReportsService.generateAnalyticsReport(filters);

      expect(report).toBeDefined();
      expect(report.metadata).toBeDefined();
      expect(report.metadata.source).toBe('duckdb');
      expect(report.statistics).toBeDefined();
      expect(report.trends).toBeDefined();
      expect(report.correlations).toBeDefined();
      expect(report.insights).toBeDefined();

      expect(mockDuckDBService.queryData).toHaveBeenCalled();
    });

    test('should calculate statistical measures', async () => {
      // Mock numerical data for statistics
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { value: 20.0 },
          { value: 25.0 },
          { value: 30.0 },
          { value: 22.0 },
          { value: 28.0 }
        ],
        source: 'duckdb'
      });

      const report = await ReportsService.generateAnalyticsReport({});

      expect(report.statistics.count).toBe(5);
      expect(report.statistics.mean).toBe(25.0);
      expect(report.statistics.median).toBe(25.0);
      expect(report.statistics.min).toBe(20.0);
      expect(report.statistics.max).toBe(30.0);
      expect(report.statistics.std_dev).toBeGreaterThan(0);
    });

    test('should detect trends in time series data', async () => {
      // Mock time series data with trend
      const timeSeriesData = Array.from({ length: 10 }, (_, i) => ({
        value: 20 + i * 0.5, // Increasing trend
        timestamp: new Date(Date.now() - (10 - i) * 3600000).toISOString()
      }));

      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: timeSeriesData,
        source: 'duckdb'
      });

      const report = await ReportsService.generateAnalyticsReport({});

      expect(report.trends).toBeDefined();
      expect(report.trends.direction).toBeDefined();
      expect(report.trends.strength).toBeDefined();
      expect(report.trends.slope).toBeDefined();
    });

    test('should generate insights from data patterns', async () => {
      const report = await ReportsService.generateAnalyticsReport({});

      expect(report.insights).toBeDefined();
      expect(Array.isArray(report.insights)).toBe(true);
      
      if (report.insights.length > 0) {
        const insight = report.insights[0];
        expect(insight.type).toBeDefined();
        expect(insight.confidence).toBeDefined();
        expect(insight.description).toBeDefined();
        expect(insight.impact).toBeDefined();
      }
    });
  });

  describe('Summary Reports', () => {
    test('should generate comprehensive summary report', async () => {
      const filters = {
        startDate: '2025-07-16T00:00:00Z',
        endDate: '2025-07-16T23:59:59Z'
      };

      const report = await ReportsService.generateSummaryReport(filters);

      expect(report).toBeDefined();
      expect(report.metadata).toBeDefined();
      expect(report.overview).toBeDefined();
      expect(report.sensor_summary).toBeDefined();
      expect(report.data_quality).toBeDefined();
      expect(report.key_metrics).toBeDefined();
      expect(report.alerts).toBeDefined();

      expect(mockDuckDBService.getStats).toHaveBeenCalled();
    });

    test('should include sensor overview', async () => {
      const report = await ReportsService.generateSummaryReport({});

      expect(report.sensor_summary).toBeDefined();
      expect(report.sensor_summary.total_sensors).toBeDefined();
      expect(report.sensor_summary.active_sensors).toBeDefined();
      expect(report.sensor_summary.by_type).toBeDefined();
      expect(report.sensor_summary.by_field).toBeDefined();
    });

    test('should generate system alerts', async () => {
      // Mock data that should generate alerts
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [
          { sensor_id: 'SENSOR_001', value: null }, // Missing data alert
          { sensor_id: 'SENSOR_002', value: 999 }, // Extreme value alert
        ],
        source: 'duckdb'
      });

      const report = await ReportsService.generateSummaryReport({});

      expect(report.alerts).toBeDefined();
      expect(Array.isArray(report.alerts)).toBe(true);
      
      if (report.alerts.length > 0) {
        const alert = report.alerts[0];
        expect(alert.severity).toBeDefined();
        expect(alert.type).toBeDefined();
        expect(alert.message).toBeDefined();
        expect(alert.timestamp).toBeDefined();
      }
    });
  });

  describe('Custom Reports', () => {
    test('should generate custom report with user-defined parameters', async () => {
      const customParams = {
        report_type: 'custom_sensor_analysis',
        sensors: ['SENSOR_001', 'SENSOR_002'],
        metrics: ['average', 'maximum', 'trend'],
        groupBy: 'daily',
        filters: {
          reading_type: 'temperature',
          startDate: '2025-07-16T00:00:00Z',
          endDate: '2025-07-16T23:59:59Z'
        }
      };

      const report = await ReportsService.generateCustomReport(customParams);

      expect(report).toBeDefined();
      expect(report.metadata).toBeDefined();
      expect(report.metadata.report_type).toBe('custom_sensor_analysis');
      expect(report.results).toBeDefined();
      expect(report.parameters).toEqual(customParams);
    });

    test('should handle invalid custom report parameters', async () => {
      const invalidParams = {
        report_type: 'unknown_type',
        invalid_field: 'invalid_value'
      };

      await expect(
        ReportsService.generateCustomReport(invalidParams)
      ).rejects.toThrow();
    });
  });

  describe('Report Storage and Retrieval', () => {
    test('should store generated reports', async () => {
      const report = await ReportsService.generateQualityReport({});
      
      expect(report.metadata.report_id).toBeDefined();
      expect(report.metadata.generated_at).toBeDefined();
      expect(report.metadata.file_path).toBeDefined();
    });

    test('should list available reports', async () => {
      const reports = await ReportsService.listReports();

      expect(reports).toBeDefined();
      expect(Array.isArray(reports)).toBe(true);
      expect(reports.length).toBeGreaterThan(0);
      
      if (reports.length > 0) {
        const report = reports[0];
        expect(report.report_id).toBeDefined();
        expect(report.type).toBeDefined();
        expect(report.generated_at).toBeDefined();
        expect(report.file_path).toBeDefined();
      }
    });

    test('should retrieve specific report by ID', async () => {
      // First generate a report
      const generatedReport = await ReportsService.generateQualityReport({});
      const reportId = generatedReport.metadata.report_id;

      // Then retrieve it
      const retrievedReport = await ReportsService.getReport(reportId);

      expect(retrievedReport).toBeDefined();
      expect(retrievedReport.metadata.report_id).toBe(reportId);
    });

    test('should handle non-existent report retrieval', async () => {
      await expect(
        ReportsService.getReport('non_existent_report_id')
      ).rejects.toThrow();
    });
  });

  describe('Report Export', () => {
    test('should export report as JSON', async () => {
      const report = await ReportsService.generateQualityReport({});
      const exported = await ReportsService.exportReport(report.metadata.report_id, 'json');

      expect(exported).toBeDefined();
      expect(typeof exported).toBe('string');
      
      const parsedReport = JSON.parse(exported);
      expect(parsedReport.metadata).toBeDefined();
      expect(parsedReport.summary).toBeDefined();
    });

    test('should export report as CSV for tabular data', async () => {
      const report = await ReportsService.generateQualityReport({});
      const exported = await ReportsService.exportReport(report.metadata.report_id, 'csv');

      expect(exported).toBeDefined();
      expect(typeof exported).toBe('string');
      expect(exported).toContain(','); // Should contain CSV delimiters
    });

    test('should handle unsupported export formats', async () => {
      const report = await ReportsService.generateQualityReport({});
      
      await expect(
        ReportsService.exportReport(report.metadata.report_id, 'xml')
      ).rejects.toThrow('Unsupported export format');
    });
  });

  describe('Performance and Optimization', () => {
    test('should handle large datasets efficiently', async () => {
      // Mock large dataset
      const largeDataset = Array.from({ length: 10000 }, (_, i) => ({
        sensor_id: `SENSOR_${i % 100}`,
        value: Math.random() * 100,
        timestamp: new Date(Date.now() - i * 60000).toISOString()
      }));

      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: largeDataset,
        source: 'duckdb'
      });

      const startTime = Date.now();
      const report = await ReportsService.generateAnalyticsReport({});
      const endTime = Date.now();

      expect(report).toBeDefined();
      expect(endTime - startTime).toBeLessThan(10000); // Should complete in under 10 seconds
    });

    test('should cache report results for repeated queries', async () => {
      const filters = { sensor_id: 'SENSOR_001' };

      // Generate report twice with same filters
      const report1 = await ReportsService.generateQualityReport(filters);
      const report2 = await ReportsService.generateQualityReport(filters);

      expect(report1).toBeDefined();
      expect(report2).toBeDefined();
      
      // Should use cached results (implementation dependent)
      // This test validates that caching doesn't break functionality
    });
  });

  describe('Error Handling', () => {
    test('should handle DuckDB query failures', async () => {
      mockDuckDBService.queryData.mockRejectedValueOnce(new Error('Query failed'));

      await expect(
        ReportsService.generateQualityReport({})
      ).rejects.toThrow('Query failed');
    });

    test('should handle DuckDB initialization failures', async () => {
      mockDuckDBService.initialize.mockRejectedValueOnce(new Error('Init failed'));

      await expect(
        ReportsService.generateAnalyticsReport({})
      ).rejects.toThrow('Init failed');
    });

    test('should handle empty datasets gracefully', async () => {
      mockDuckDBService.queryData.mockResolvedValueOnce({
        success: true,
        data: [],
        source: 'duckdb'
      });

      const report = await ReportsService.generateQualityReport({});

      expect(report).toBeDefined();
      expect(report.summary.total_records).toBe(0);
      expect(report.quality_metrics.completeness_rate).toBe(0);
    });
  });

  describe('Service Lifecycle', () => {
    test('should close reports service properly', async () => {
      await ReportsService.close();
      expect(mockDuckDBService.close).toHaveBeenCalled();
    });

    test('should clear report cache', () => {
      const result = ReportsService.clearCache();
      expect(result.success).toBe(true);
    });
  });
});
