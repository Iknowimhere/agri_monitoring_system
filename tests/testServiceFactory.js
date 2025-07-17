// Test utilities for service isolation
const DuckDBService = require('../src/services/duckDBService');
const DataService = require('../src/services/dataService');
const PipelineService = require('../src/services/pipelineService');

class TestServiceFactory {
  static createDuckDBService(testId) {
    const testConfig = global.testUtils.getTestConfig(testId);
    return new DuckDBService(testConfig);
  }
  
  static async createDataService(testId) {
    const testConfig = global.testUtils.getTestConfig(testId);
    const dataService = Object.create(DataService);
    
    // Replace the duckDBService with test-specific instance
    dataService.duckDBService = new DuckDBService(testConfig);
    dataService.initialized = false;
    
    return dataService;
  }

  static createPipelineService(testId) {
    const testConfig = global.testUtils.getTestConfig(testId);
    
    // Create a test-specific pipeline service
    const pipelineService = new PipelineService();
    
    // Override the DuckDB service with test-specific instance
    pipelineService.duckDBService = new DuckDBService(testConfig);
    pipelineService.testConfig = testConfig;
    
    return pipelineService;
  }
}

module.exports = TestServiceFactory;
