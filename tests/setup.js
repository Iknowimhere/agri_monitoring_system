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