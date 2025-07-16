// Global test setup
process.env.NODE_ENV = 'test';
process.env.LOG_LEVEL = 'error'; // Reduce logging during tests

// Mock external dependencies if needed
jest.setTimeout(30000); // 30 second timeout for tests
