// Global test setup
process.env.NODE_ENV = 'test';
process.env.LOG_LEVEL = 'error'; // Reduce logging during tests

// Mock external dependencies if needed
jest.setTimeout(60000); // 60 second timeout for tests

const fs = require('fs').promises;
const path = require('path');

// Global test utilities
global.testUtils = {
  async cleanupTestDatabases() {
    try {
      const dataDir = path.join(process.cwd(), 'data', 'database');
      const files = await fs.readdir(dataDir);
      
      for (const file of files) {
        if (file.includes('test_') && (file.endsWith('.duckdb') || file.endsWith('.sqlite'))) {
          try {
            await fs.unlink(path.join(dataDir, file));
          } catch (error) {
            // Ignore cleanup errors
          }
        }
      }
    } catch (error) {
      // Directory doesn't exist or other error, ignore
    }
  },
  
  async cleanupTestDirectories() {
    try {
      const dataDir = path.join(process.cwd(), 'data');
      const entries = await fs.readdir(dataDir);
      
      for (const entry of entries) {
        if (entry.startsWith('test-processed-')) {
          try {
            await fs.rm(path.join(dataDir, entry), { recursive: true, force: true });
          } catch (error) {
            // Ignore cleanup errors
          }
        }
      }
    } catch (error) {
      // Directory doesn't exist or other error, ignore
    }
  },

  // Generate unique test ID for each test run
  generateTestId() {
    return `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  },
  
  getTestConfig(testId) {
    const baseConfig = require('../src/config/config');
    const uniqueTestId = testId || global.testUtils.generateTestId();
    
    return {
      ...baseConfig,
      testId: uniqueTestId,
      database: {
        ...baseConfig.database,
        duckdbPath: path.join(process.cwd(), 'data', 'database', `test_${uniqueTestId}.duckdb`),
        sqlitePath: path.join(process.cwd(), 'data', 'database', `test_${uniqueTestId}.sqlite`)
      },
      paths: {
        ...baseConfig.paths,
        processedData: path.join(process.cwd(), 'data', `test-processed-${uniqueTestId}`),
        rawData: baseConfig.paths.rawData // Keep shared raw data
      }
    };
  },
  
  async cleanupTestFiles(testId) {
    if (!testId) return;
    
    try {
      const dataDir = path.join(process.cwd(), 'data');
      
      // Clean up test databases
      const dbDir = path.join(dataDir, 'database');
      const dbFiles = await fs.readdir(dbDir).catch(() => []);
      
      for (const file of dbFiles) {
        if (file.includes(`test_${testId}`) && (file.endsWith('.duckdb') || file.endsWith('.sqlite'))) {
          try {
            await fs.unlink(path.join(dbDir, file));
          } catch (error) {
            // Ignore cleanup errors
          }
        }
      }
      
      // Clean up test processed directories  
      const processedDir = path.join(dataDir, `test-processed-${testId}`);
      try {
        await fs.rm(processedDir, { recursive: true, force: true });
      } catch (error) {
        // Ignore cleanup errors
      }
      
    } catch (error) {
      // Ignore cleanup errors
    }
  }
};

// Cleanup before and after all tests
beforeAll(async () => {
  await global.testUtils.cleanupTestDatabases();
  await global.testUtils.cleanupTestDirectories();
});

afterAll(async () => {
  await global.testUtils.cleanupTestDatabases();
  await global.testUtils.cleanupTestDirectories();
});