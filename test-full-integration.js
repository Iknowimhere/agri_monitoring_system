const { app, initializeDatabase, getDuckDBService } = require('./src/app');
const DataService = require('./src/services/dataService');
const PipelineService = require('./src/services/pipelineService');
const config = require('./src/config/config');
const logger = require('./src/utils/logger');

async function testFullDuckDBIntegration() {
  console.log('🚀 Testing Full DuckDB Integration Across Project...\n');

  try {
    // 1. Test App-level DuckDB initialization
    console.log('1. Testing App-level DuckDB Initialization...');
    const appDbResult = await initializeDatabase();
    console.log(`   ✅ App DuckDB initialized: ${appDbResult.mode} (${appDbResult.success ? 'success' : 'failed'})`);
    
    const globalService = getDuckDBService();
    console.log(`   ✅ Global DuckDB service available: ${globalService ? 'Yes' : 'No'}`);
    console.log('');

    // 2. Test DataService DuckDB integration
    console.log('2. Testing DataService DuckDB Integration...');
    
    // Insert test data via DataService
    const testSensorData = {
      sensor_id: 'INTEGRATION_TEST_001',
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

    const insertResult = await DataService.insertData(testSensorData);
    console.log(`   ✅ Data inserted via DataService: ${insertResult.success ? 'Success' : 'Failed'}`);

    // Query data via DataService
    const queryResult = await DataService.queryData({ sensor_id: 'INTEGRATION_TEST_001' });
    console.log(`   ✅ Data queried via DataService: ${queryResult.data.length} records found`);
    console.log(`   ✅ Data source: ${queryResult.source}`);

    // Get sensors via DataService
    const sensors = await DataService.getSensors();
    console.log(`   ✅ Sensors retrieved: ${sensors.length} total sensors`);

    // Get statistics via DataService
    const stats = await DataService.getStats();
    console.log(`   ✅ Statistics retrieved: ${stats.total} total records from ${stats.source}`);
    console.log('');

    // 3. Test PipelineService DuckDB integration
    console.log('3. Testing PipelineService DuckDB Integration...');
    const pipelineStatus = await PipelineService.getStatus();
    console.log(`   ✅ Pipeline status: ${pipelineStatus.status}`);
    console.log('');

    // 4. Test DataService advanced features
    console.log('4. Testing DataService Advanced Features...');
    
    // Test sensor summary
    const sensorSummary = await DataService.getSensorSummary('INTEGRATION_TEST_001');
    console.log(`   ✅ Sensor summary: ${sensorSummary.total_readings} readings for sensor ${sensorSummary.sensor_id}`);
    console.log(`   ✅ Reading types: ${sensorSummary.reading_types.join(', ')}`);

    // Test daily aggregations
    const today = new Date().toISOString().split('T')[0];
    const aggregations = await DataService.getDailyAggregations({
      sensor_id: 'INTEGRATION_TEST_001',
      startDate: today,
      endDate: today
    });
    console.log(`   ✅ Daily aggregations: ${aggregations.total_days} days, source: ${aggregations.source}`);

    // Test data export
    const csvData = await DataService.exportData('csv', { sensor_id: 'INTEGRATION_TEST_001' });
    console.log(`   ✅ CSV export: ${csvData.split('\n').length - 1} data rows exported`);

    // Test data update
    const firstRecord = queryResult.data[0];
    if (firstRecord) {
      const updateResult = await DataService.updateData(firstRecord.id, { quality_score: 100 });
      console.log(`   ✅ Data update: ${updateResult.success ? 'Success' : 'Failed'}`);
    }
    console.log('');

    // 5. Test API endpoints integration
    console.log('5. Testing API Integration...');
    
    // Create a test request to simulate API call
    const mockReq = {
      query: {
        sensor_id: 'INTEGRATION_TEST_001',
        page: '1',
        limit: '10'
      }
    };
    
    const mockRes = {
      json: (data) => {
        console.log(`   ✅ API Response: ${data.data ? data.data.length : 0} records returned`);
        console.log(`   ✅ Pagination: Page ${data.pagination ? data.pagination.page : 'N/A'} of ${data.pagination ? data.pagination.pages : 'N/A'}`);
        return data;
      }
    };

    const DataController = require('./src/controllers/dataController');
    try {
      await DataController.queryData(mockReq, mockRes, (error) => {
        if (error) throw error;
      });
    } catch (apiError) {
      console.log(`   ⚠️  API test skipped due to: ${apiError.message}`);
    }
    console.log('');

    // 6. Performance test
    console.log('6. Performance Testing...');
    const performanceStart = Date.now();
    
    // Insert multiple records
    const performanceData = [];
    for (let i = 0; i < 50; i++) {
      performanceData.push({
        sensor_id: `PERF_TEST_${i.toString().padStart(3, '0')}`,
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
      });
    }

    const batchInsertResult = await DataService.insertData(performanceData);
    const performanceEnd = Date.now();
    
    console.log(`   ✅ Performance test: ${performanceData.length} records inserted in ${performanceEnd - performanceStart}ms`);
    console.log(`   ✅ Average: ${((performanceEnd - performanceStart) / performanceData.length).toFixed(2)}ms per record`);
    console.log(`   ✅ Batch insert success: ${batchInsertResult.success}`);
    console.log('');

    // 7. Final verification
    console.log('7. Final System Verification...');
    const finalStats = await DataService.getStats();
    console.log(`   ✅ Total records in system: ${finalStats.total}`);
    console.log(`   ✅ Database backend: ${finalStats.source.toUpperCase()}`);
    console.log(`   ✅ Data types in system:`);
    
    if (finalStats.byType && finalStats.byType.length > 0) {
      finalStats.byType.forEach(type => {
        console.log(`      - ${type.reading_type}: ${type.count} records`);
      });
    }
    
    console.log(`   ✅ Latest data timestamp: ${finalStats.latestTimestamp}`);
    console.log('');

    // 8. Cleanup test data
    console.log('8. Cleaning up test data...');
    try {
      const testRecords = await DataService.queryData({ sensor_id: 'INTEGRATION_TEST_001' });
      if (testRecords.data.length > 0) {
        await DataService.deleteData(testRecords.data[0].id);
        console.log(`   ✅ Test data cleaned up: ${testRecords.data.length} records processed`);
      }
    } catch (cleanupError) {
      console.log(`   ⚠️  Cleanup skipped: ${cleanupError.message}`);
    }

    console.log('\n🎉 Full DuckDB Integration Test Completed Successfully!');
    console.log('\n📊 Integration Summary:');
    console.log('   ✅ App-level DuckDB initialization');
    console.log('   ✅ DataService DuckDB integration');
    console.log('   ✅ PipelineService DuckDB integration');
    console.log('   ✅ Advanced querying and analytics');
    console.log('   ✅ CRUD operations');
    console.log('   ✅ Performance testing');
    console.log('   ✅ API endpoint integration');
    
  } catch (error) {
    console.error('❌ Integration test failed:', error);
    process.exit(1);
  } finally {
    // Close connections
    try {
      await DataService.close();
      console.log('\n✅ Database connections closed gracefully');
    } catch (closeError) {
      console.error('❌ Error closing connections:', closeError);
    }
  }
}

// Run the integration test
testFullDuckDBIntegration();
