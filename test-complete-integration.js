const DuckDBService = require('./src/services/duckDBService');
const path = require('path');

async function runCompleteIntegrationTest() {
  console.log('🚀 Running Complete DuckDB Integration Test...\n');
  
  // Create a simple config for testing
  const testConfig = {
    paths: {
      processedData: path.join(__dirname, 'data', 'processed')
    }
  };
  
  const dbService = new DuckDBService(testConfig);
  
  try {
    // Initialize service
    console.log('1. Initializing Database Service...');
    await dbService.initialize();
    console.log(`   ✅ Service initialized (Mode: ${dbService.dbType.toUpperCase()})\n`);
    
    // Test multiple data insertions
    console.log('2. Testing Multiple Data Insertions...');
    const testData = [
      {
        sensor_id: 'SOIL_001',
        location: { 
          field: 'field_001', 
          latitude: 40.7128, 
          longitude: -74.0060 
        },
        reading_type: 'soil_moisture',
        value: 85.5,
        unit: 'percentage',
        quality_score: 95,
        timestamp: new Date().toISOString(),
        battery_level: 85,
        signal_strength: 90,
        data_quality: 'good'
      },
      {
        sensor_id: 'TEMP_002',
        location: { 
          field: 'field_002', 
          latitude: 40.7580, 
          longitude: -73.9855 
        },
        reading_type: 'temperature',
        value: 22.3,
        unit: 'celsius',
        quality_score: 88,
        timestamp: new Date().toISOString(),
        battery_level: 78,
        signal_strength: 85,
        data_quality: 'fair'
      },
      {
        sensor_id: 'HUMID_003',
        location: { 
          field: 'field_003', 
          latitude: 40.6892, 
          longitude: -74.0445 
        },
        reading_type: 'humidity',
        value: 67.8,
        unit: 'percentage',
        quality_score: 92,
        timestamp: new Date().toISOString(),
        battery_level: 92,
        signal_strength: 95,
        data_quality: 'excellent'
      }
    ];
    
    for (let i = 0; i < testData.length; i++) {
      const result = await dbService.insertData(testData[i]);
      console.log(`   ✅ Record ${i + 1} inserted (ID: ${result.id})`);
    }
    console.log('');
    
    // Test data querying
    console.log('3. Testing Data Queries...');
    
    // Query all data
    const allData = await dbService.queryData({});
    console.log(`   ✅ All data query: ${allData.data.length} records found`);
    
    // Query by type
    const soilData = await dbService.queryData({ reading_type: 'soil_moisture' });
    console.log(`   ✅ Soil moisture query: ${soilData.data.length} records found`);
    
    // Query with limit
    const limitedData = await dbService.queryData({}, 2);
    console.log(`   ✅ Limited query: ${limitedData.data.length} records found (limit: 2)`);
    console.log('');
    
    // Test statistics
    console.log('4. Testing Statistics...');
    const stats = await dbService.getStats();
    console.log(`   ✅ Total records: ${stats.total}`);
    console.log(`   ✅ Data source: ${stats.source}`);
    console.log(`   ✅ Types breakdown:`);
    stats.byType.forEach(type => {
      console.log(`      - ${type.reading_type}: ${type.count} records`);
    });
    console.log(`   ✅ Latest timestamp: ${stats.latestTimestamp}`);
    console.log('');
    
    // Test data update
    console.log('5. Testing Data Updates...');
    const firstRecord = allData.data[0];
    const updateResult = await dbService.updateData(firstRecord.id, {
      quality_score: 99,
      battery_level: 100
    });
    console.log(`   ✅ Record updated: ${updateResult.success ? 'Success' : 'Failed'}`);
    console.log('');
    
    // Test data deletion
    console.log('6. Testing Data Deletion...');
    const deleteResult = await dbService.deleteData(firstRecord.id);
    console.log(`   ✅ Record deleted: ${deleteResult.success ? 'Success' : 'Failed'}`);
    
    // Final stats check
    const finalStats = await dbService.getStats();
    console.log(`   ✅ Final record count: ${finalStats.total}`);
    console.log('');
    
    // Performance test
    console.log('7. Performance Test (100 insertions)...');
    const startTime = Date.now();
    const performancePromises = [];
    
    for (let i = 0; i < 100; i++) {
      performancePromises.push(dbService.insertData({
        sensor_id: `PERF_${i.toString().padStart(3, '0')}`,
        location: { 
          field: `perf_field_${i}`,
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
    }
    
    await Promise.all(performancePromises);
    const endTime = Date.now();
    console.log(`   ✅ 100 records inserted in ${endTime - startTime}ms`);
    console.log(`   ✅ Average: ${((endTime - startTime) / 100).toFixed(2)}ms per record`);
    console.log('');
    
    // Final verification
    const performanceStats = await dbService.getStats();
    console.log(`8. Final Verification:`);
    console.log(`   ✅ Total records in database: ${performanceStats.total}`);
    console.log(`   ✅ Database type: ${performanceStats.source}`);
    console.log('');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  } finally {
    try {
      await dbService.close();
      console.log('✅ Database connection closed');
    } catch (closeError) {
      console.error('❌ Error closing database:', closeError);
    }
  }
  
  console.log('\n🎉 Complete Integration Test Finished!');
}

// Run the test
runCompleteIntegrationTest().catch(console.error);
