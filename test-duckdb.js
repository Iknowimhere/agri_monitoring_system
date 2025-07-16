const buildToolsChecker = require('./src/utils/buildToolsChecker');
const DuckDBService = require('./src/services/duckDBService');
const config = require('./src/config/config');

async function testDuckDBImplementation() {
  console.log('🔧 Testing DuckDB Implementation...\n');

  // Check build tools
  console.log('1. Checking C++ Build Tools...');
  const recommendations = await buildToolsChecker.getRecommendations();
  
  console.log('Build Tools Available:', recommendations.buildTools ? '✅' : '❌');
  console.log('DuckDB Working:', recommendations.duckdb ? '✅' : '❌');
  
  if (recommendations.recommendations.length > 0) {
    console.log('\n📋 Recommendations:');
    recommendations.recommendations.forEach(rec => console.log(`  - ${rec}`));
  }

  // Test DuckDB Service
  console.log('\n2. Testing Database Service...');
  const duckDBService = new DuckDBService(config);
  
  try {
    const initResult = await duckDBService.initialize();
    console.log('Service initialized:', initResult.success ? '✅' : '❌');
    console.log('Database mode:', initResult.mode.toUpperCase());
    console.log('Using database:', duckDBService.isUsingDuckDB() ? 'SQL Database' : 'File-based');
    
    if (initResult.error) {
      console.log('Note:', initResult.error);
    }

    // Test data insertion
    console.log('\n3. Testing Data Operations...');
    const testData = [{
      sensor_id: 'test-sensor-01',
      timestamp: new Date().toISOString(),
      reading_type: 'temperature',
      value: 25.5,
      unit: '°C',
      location: { field: 'Test-Field', latitude: 28.6139, longitude: 77.2090 },
      battery_level: 85,
      signal_strength: 92,
      data_quality: 'good',
      quality_score: 100
    }];

    const insertResult = await duckDBService.insertData(testData);
    console.log('Data insertion:', insertResult.success ? '✅' : '❌');
    console.log('Records inserted:', insertResult.recordCount);

    // Test querying
    const queryResult = await duckDBService.queryData({ limit: 5 });
    console.log('Data query:', queryResult.success ? '✅' : '❌');
    console.log('Query source:', queryResult.source);
    console.log('Records found:', queryResult.data.length);

    // Test stats
    const stats = await duckDBService.getStats();
    console.log('Statistics:', stats.success ? '✅' : '❌');
    console.log('Total records:', stats.total);
    console.log('Data source:', stats.source);

    await duckDBService.close();
    
    console.log('\n🎉 DuckDB Implementation Test Complete!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

if (require.main === module) {
  testDuckDBImplementation().catch(console.error);
}

module.exports = { testDuckDBImplementation };
