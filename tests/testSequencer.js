const Sequencer = require('@jest/test-sequencer').default;

class CustomSequencer extends Sequencer {
  sort(tests) {
    // Sort tests to run in logical order
    const testOrder = [
      'duckdb.test.js',        // Core DuckDB tests first
      'dataService.test.js',   // Data service tests
      'pipelineService.test.js', // Pipeline service tests  
      'reportsService.test.js', // Reports service tests
      'integration.test.js',   // Integration tests last
      'api.test.js',          // API tests
      'services.test.js'      // Other service tests
    ];

    return tests.sort((testA, testB) => {
      const aName = testA.path.split('/').pop();
      const bName = testB.path.split('/').pop();
      
      const aIndex = testOrder.indexOf(aName);
      const bIndex = testOrder.indexOf(bName);
      
      // If both tests are in our order list, sort by order
      if (aIndex !== -1 && bIndex !== -1) {
        return aIndex - bIndex;
      }
      
      // If only one test is in our order list, prioritize it
      if (aIndex !== -1) return -1;
      if (bIndex !== -1) return 1;
      
      // For tests not in our list, sort alphabetically
      return aName.localeCompare(bName);
    });
  }
}

module.exports = CustomSequencer;
