const logger = require('../utils/logger');
const buildToolsChecker = require('../utils/buildToolsChecker');
const path = require('path');
const fs = require('fs').promises;

class DuckDBService {
  constructor(config = {}) {
    this.config = config;
    this.db = null;
    this.isAvailable = false;
    this.dbType = null; // 'duckdb', 'sqlite', or 'fallback'
    
    // Create proper data paths with test isolation
    const dataRoot = path.join(process.cwd(), 'data');
    
    // Use test-specific paths if in test environment
    if (process.env.NODE_ENV === 'test') {
      const testId = config.testId || Date.now();
      this.duckdbPath = config.duckdbPath || path.join(dataRoot, 'database', `test_${testId}.duckdb`);
      this.sqlitePath = config.sqlitePath || path.join(dataRoot, 'database', `test_${testId}.sqlite`);
      this.fallbackPath = config.paths?.processedData || path.join(dataRoot, 'test-processed');
    } else {
      this.duckdbPath = config.duckdbPath || path.join(dataRoot, 'database', 'agricultural_data.duckdb');
      this.sqlitePath = config.sqlitePath || path.join(dataRoot, 'database', 'agricultural_data.sqlite');
      this.fallbackPath = (config && config.paths && config.paths.processedData) ? config.paths.processedData : path.join(dataRoot, 'processed');
    }
  }

  async initialize() {
    try {
      logger.info('Initializing database service...');
      
      // Try DuckDB first
      const duckdbAvailable = await this.checkDuckDB();
      if (duckdbAvailable) {
        logger.info('DuckDB is available, initializing...');
        await this.initializeDuckDB();
        return { 
          success: true, 
          mode: 'duckdb',
          database: 'duckdb',
          dbPath: this.duckdbPath
        };
      } else {
        logger.warn('DuckDB not available, checking SQLite...');
      }

      // Try SQLite as fallback
      const sqliteAvailable = await this.checkSQLite();
      if (sqliteAvailable) {
        logger.info('SQLite is available, initializing...');
        await this.initializeSQLite();
        return { 
          success: true, 
          mode: 'sqlite',
          database: 'sqlite',
          dbPath: this.sqlitePath
        };
      } else {
        logger.warn('SQLite not available, using file-based fallback');
      }

      // Use file-based fallback
      logger.warn('Neither DuckDB nor SQLite available, using file-based fallback');
      await this.initializeFallback();
      return { 
        success: true, 
        mode: 'fallback',
        database: 'fallback',
        dbPath: this.fallbackPath
      };
      
    } catch (error) {
      logger.error('Database service initialization failed:', error);
      await this.initializeFallback();
      return { 
        success: true, 
        mode: 'fallback', 
        database: 'fallback',
        dbPath: this.fallbackPath,
        error: error.message 
      };
    }
  }

  async checkDuckDB() {
    try {
      logger.info('Testing DuckDB availability...');
      
      // First, check if DuckDB module can be required
      let duckdb;
      try {
        duckdb = require('duckdb');
        logger.info('DuckDB module loaded successfully');
      } catch (requireError) {
        logger.error('DuckDB module failed to load:', requireError.message);
        return false;
      }
      
      // Test with database creation (increase timeout for containers)
      return await Promise.race([
        new Promise((resolve) => {
          try {
            const testDb = new duckdb.Database(':memory:', (err) => {
              if (err) {
                logger.error('DuckDB connection test failed:', err.message);
                resolve(false);
              } else {
                logger.info('✅ DuckDB connection test successful');
                try {
                  testDb.close();
                } catch (closeError) {
                  logger.warn('DuckDB close warning:', closeError.message);
                }
                resolve(true);
              }
            });
          } catch (error) {
            logger.error('DuckDB database creation failed:', error.message);
            resolve(false);
          }
        }),
        new Promise((resolve) => setTimeout(() => {
          logger.warn('DuckDB connection test timed out after 10 seconds');
          resolve(false);
        }, 10000)) // Increased timeout to 10 seconds for Docker
      ]);
    } catch (error) {
      logger.error('DuckDB check failed:', error.message);
      return false;
    }
  }

  async checkSQLite() {
    try {
      logger.info('Testing SQLite availability...');
      const Database = require('better-sqlite3');
      const testDb = new Database(':memory:');
      testDb.close();
      logger.info('✅ SQLite is available');
      return true;
    } catch (error) {
      logger.warn('SQLite not available:', error.message);
      return false;
    }
  }

  async initializeDuckDB() {
    const duckdb = require('duckdb');
    
    // Ensure database directory exists with proper permissions
    try {
      await fs.mkdir(path.dirname(this.duckdbPath), { recursive: true });
      logger.info('Database directory created:', path.dirname(this.duckdbPath));
    } catch (error) {
      logger.warn('Database directory creation warning:', error.message);
    }
    
    return new Promise((resolve, reject) => {
      logger.info('Creating DuckDB database at:', this.duckdbPath);
      this.db = new duckdb.Database(this.duckdbPath, (err) => {
        if (err) {
          logger.error('DuckDB database creation failed:', err.message);
          reject(err);
        } else {
          this.isAvailable = true;
          this.dbType = 'duckdb';
          logger.info('✅ DuckDB initialized successfully', { dbPath: this.duckdbPath });
          this.createDuckDBTables().then(resolve).catch(reject);
        }
      });
    });
  }

  async initializeSQLite() {
    const Database = require('better-sqlite3');
    
    // Ensure database directory exists
    await fs.mkdir(path.dirname(this.sqlitePath), { recursive: true });
    
    this.db = new Database(this.sqlitePath);
    this.isAvailable = true;
    this.dbType = 'sqlite';
    
    logger.info('✅ SQLite initialized successfully', { dbPath: this.sqlitePath });
    await this.createSQLiteTables();
  }

  async initializeFallback() {
    await fs.mkdir(this.fallbackPath, { recursive: true });
    this.isAvailable = false;
    logger.info('File-based fallback initialized');
  }

  async createDuckDBTables() {
    if (this.dbType !== 'duckdb') return;

    // Drop table if exists to recreate with proper schema
    const dropSQL = `DROP TABLE IF EXISTS sensor_data`;
    const createTableSQL = `
      CREATE TABLE sensor_data (
        id BIGINT PRIMARY KEY DEFAULT nextval('sensor_data_seq'),
        sensor_id VARCHAR,
        timestamp TIMESTAMP,
        reading_type VARCHAR,
        value DOUBLE,
        unit VARCHAR,
        field VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE,
        battery_level INTEGER,
        signal_strength INTEGER,
        data_quality VARCHAR,
        processed_timestamp TIMESTAMP,
        quality_score INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;

    const createSequenceSQL = `CREATE SEQUENCE IF NOT EXISTS sensor_data_seq START 1`;

    return new Promise((resolve, reject) => {
      this.db.run(createSequenceSQL, (err) => {
        if (err) {
          reject(err);
        } else {
          this.db.run(dropSQL, (err) => {
            if (err) {
              reject(err);
            } else {
              this.db.run(createTableSQL, (err) => {
                if (err) {
                  reject(err);
                } else {
                  logger.info('DuckDB tables created successfully');
                  resolve();
                }
              });
            }
          });
        }
      });
    });
  }

  async createSQLiteTables() {
    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id TEXT,
        timestamp TEXT,
        reading_type TEXT,
        value REAL,
        unit TEXT,
        field TEXT,
        latitude REAL,
        longitude REAL,
        battery_level INTEGER,
        signal_strength INTEGER,
        data_quality TEXT,
        processed_timestamp TEXT,
        quality_score INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `;

    this.db.exec(createTableSQL);
    logger.info('SQLite tables created successfully');
  }

  async insertData(records) {
    // Handle single record by wrapping in array
    if (!Array.isArray(records)) {
      records = [records];
    }
    
    if (!this.isAvailable) {
      return this.insertDataFallback(records);
    }

    try {
      if (this.dbType === 'sqlite') {
        return await this.insertDataSQLite(records);
      } else if (this.dbType === 'duckdb') {
        return await this.insertDataDuckDB(records);
      }
    } catch (error) {
      logger.error(`Failed to insert data into ${this.dbType}:`, error);
      throw error;
    }
  }

  async insertDataSQLite(records) {
    const insertSQL = `
      INSERT INTO sensor_data (
        sensor_id, timestamp, reading_type, value, unit, 
        field, latitude, longitude, battery_level, signal_strength,
        data_quality, processed_timestamp, quality_score
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    const insert = this.db.prepare(insertSQL);
    
    for (const record of records) {
      insert.run([
        record.sensor_id,
        record.timestamp,
        record.reading_type,
        record.value,
        record.unit,
        record.location?.field,
        record.location?.latitude,
        record.location?.longitude,
        record.battery_level,
        record.signal_strength,
        record.data_quality,
        record.processed_timestamp || new Date().toISOString(),
        record.quality_score || 100
      ]);
    }

    logger.info('Data inserted into SQLite', { recordCount: records.length });
    return { success: true, recordCount: records.length };
  }

  async insertDataDuckDB(records) {
    const insertSQL = `
      INSERT INTO sensor_data (
        sensor_id, timestamp, reading_type, value, unit, 
        field, latitude, longitude, battery_level, signal_strength,
        data_quality, processed_timestamp, quality_score
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    `;

    const results = [];
    for (const record of records) {
      const params = [
        record.sensor_id || null,
        record.timestamp || new Date().toISOString(),
        record.reading_type || null,
        record.value || null,
        record.unit || null,
        record.location?.field || record.field || null,
        record.location?.latitude || null,
        record.location?.longitude || null,
        record.battery_level || null,
        record.signal_strength || null,
        record.data_quality || 'unknown',
        record.processed_timestamp || new Date().toISOString(),
        record.quality_score || 100
      ];
      
      const result = await this.runDuckDBQuery(insertSQL, params);
      results.push(result);
    }

    logger.info('Data inserted into DuckDB', { recordCount: records.length });
    return { success: true, recordCount: records.length };
  }

  async insertDataFallback(records) {
    try {
      // Ensure the fallback directory exists
      await fs.mkdir(this.fallbackPath, { recursive: true });
      
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const filename = `processed_data_${timestamp}.json`;
      const filepath = path.join(this.fallbackPath, filename);
      
      await fs.writeFile(filepath, JSON.stringify(records, null, 2));
      
      logger.info('Data saved to fallback file', { filepath, recordCount: records.length });
      return { success: true, recordCount: records.length, filepath };
    } catch (error) {
      logger.error('Failed to save data to fallback:', error);
      throw error;
    }
  }

  async queryData(filters = {}) {
    if (!this.isAvailable) {
      return this.queryDataFallback(filters);
    }

    try {
      if (this.dbType === 'sqlite') {
        return await this.queryDataSQLite(filters);
      } else if (this.dbType === 'duckdb') {
        return await this.queryDataDuckDB(filters);
      }
    } catch (error) {
      logger.error(`Failed to query ${this.dbType}:`, error);
      throw error;
    }
  }

  async queryDataDuckDB(filters = {}) {
    let sql = 'SELECT * FROM sensor_data WHERE 1=1';
    const params = [];

    if (filters.sensor_id) {
      sql += ' AND sensor_id = $' + (params.length + 1);
      params.push(filters.sensor_id);
    }

    if (filters.reading_type) {
      sql += ' AND reading_type = $' + (params.length + 1);
      params.push(filters.reading_type);
    }

    if (filters.startDate) {
      sql += ' AND timestamp >= $' + (params.length + 1);
      params.push(filters.startDate);
    }

    if (filters.endDate) {
      sql += ' AND timestamp <= $' + (params.length + 1);
      params.push(filters.endDate);
    }

    if (filters.field) {
      sql += ' AND field = $' + (params.length + 1);
      params.push(filters.field);
    }

    sql += ' ORDER BY timestamp DESC';

    if (filters.limit) {
      sql += ' LIMIT $' + (params.length + 1);
      params.push(filters.limit);
    }

    const results = await this.allDuckDBQuery(sql, params);
    
    // Convert BigInt values to regular numbers
    const sanitizedResults = results.map(record => {
      const sanitized = {};
      for (const [key, value] of Object.entries(record)) {
        if (typeof value === 'bigint') {
          sanitized[key] = Number(value);
        } else {
          sanitized[key] = value;
        }
      }
      return sanitized;
    });
    
    return { success: true, data: sanitizedResults, source: 'duckdb' };
  }

  async queryDataSQLite(filters = {}) {
    let sql = 'SELECT * FROM sensor_data WHERE 1=1';
    const params = [];

    if (filters.sensor_id) {
      sql += ' AND sensor_id = ?';
      params.push(filters.sensor_id);
    }

    if (filters.reading_type) {
      sql += ' AND reading_type = ?';
      params.push(filters.reading_type);
    }

    if (filters.startDate) {
      sql += ' AND timestamp >= ?';
      params.push(filters.startDate);
    }

    if (filters.endDate) {
      sql += ' AND timestamp <= ?';
      params.push(filters.endDate);
    }

    if (filters.field) {
      sql += ' AND field = ?';
      params.push(filters.field);
    }

    sql += ' ORDER BY timestamp DESC';

    if (filters.limit) {
      sql += ' LIMIT ?';
      params.push(filters.limit);
    }

    const stmt = this.db.prepare(sql);
    const results = stmt.all(params);
    return { success: true, data: results, source: 'sqlite' };
  }

  async queryDataFallback(filters = {}) {
    try {
      // Ensure the fallback directory exists
      await fs.mkdir(this.fallbackPath, { recursive: true });
      
      const files = await fs.readdir(this.fallbackPath);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      
      let allData = [];
      for (const file of jsonFiles) {
        try {
          const filepath = path.join(this.fallbackPath, file);
          const content = await fs.readFile(filepath, 'utf8');
          const data = JSON.parse(content);
          allData = allData.concat(Array.isArray(data) ? data : [data]);
        } catch (error) {
          logger.warn(`Failed to read file ${file}:`, error.message);
        }
      }

      // Apply filters
      let filteredData = allData.filter(record => {
        if (filters.sensor_id && record.sensor_id !== filters.sensor_id) return false;
        if (filters.reading_type && record.reading_type !== filters.reading_type) return false;
        if (filters.field && record.location?.field !== filters.field) return false;
        if (filters.startDate && new Date(record.timestamp) < new Date(filters.startDate)) return false;
        if (filters.endDate && new Date(record.timestamp) > new Date(filters.endDate)) return false;
        return true;
      });

      // Sort by timestamp
      filteredData.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

      // Apply limit
      if (filters.limit) {
        filteredData = filteredData.slice(0, filters.limit);
      }

      return { success: true, data: filteredData, source: 'fallback' };
    } catch (error) {
      logger.error('Failed to query fallback data:', error);
      throw error;
    }
  }

  async runQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.run(sql, params, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve({ changes: this.changes, lastID: this.lastID });
        }
      });
    });
  }

  async allQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async runDuckDBQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.run(sql, ...params, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve({ changes: this.changes, lastID: this.lastID });
        }
      });
    });
  }

  async allDuckDBQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.all(sql, ...params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async getStats() {
    if (!this.isAvailable) {
      return this.getStatsFallback();
    }

    try {
      if (this.dbType === 'sqlite') {
        return await this.getStatsSQLite();
      } else if (this.dbType === 'duckdb') {
        return await this.getStatsDuckDB();
      }
    } catch (error) {
      logger.error(`Failed to get ${this.dbType} stats:`, error);
      throw error;
    }
  }

  async getStatsDuckDB() {
    const countQuery = 'SELECT COUNT(*) as total FROM sensor_data';
    const typeQuery = 'SELECT reading_type, COUNT(*) as count FROM sensor_data GROUP BY reading_type';
    const recentQuery = 'SELECT MAX(timestamp) as latest FROM sensor_data';

    const [countResult, typeResult, recentResult] = await Promise.all([
      this.allDuckDBQuery(countQuery),
      this.allDuckDBQuery(typeQuery),
      this.allDuckDBQuery(recentQuery)
    ]);

    // Convert BigInt values to regular numbers
    const sanitizeRecord = (record) => {
      const sanitized = {};
      for (const [key, value] of Object.entries(record)) {
        if (typeof value === 'bigint') {
          sanitized[key] = Number(value);
        } else {
          sanitized[key] = value;
        }
      }
      return sanitized;
    };

    return {
      success: true,
      source: 'duckdb',
      total: countResult[0] ? Number(countResult[0].total) : 0,
      byType: typeResult.map(sanitizeRecord),
      latestTimestamp: recentResult[0]?.latest
    };
  }

  async getStatsSQLite() {
    const countStmt = this.db.prepare('SELECT COUNT(*) as total FROM sensor_data');
    const typeStmt = this.db.prepare('SELECT reading_type, COUNT(*) as count FROM sensor_data GROUP BY reading_type');
    const recentStmt = this.db.prepare('SELECT MAX(timestamp) as latest FROM sensor_data');

    const countResult = countStmt.get();
    const typeResult = typeStmt.all();
    const recentResult = recentStmt.get();

    return {
      success: true,
      source: 'sqlite',
      total: countResult?.total || 0,
      byType: typeResult,
      latestTimestamp: recentResult?.latest
    };
  }

  async getStatsFallback() {
    try {
      const queryResult = await this.queryDataFallback();
      const data = queryResult.data;

      const stats = {
        total: data.length,
        byType: {},
        latestTimestamp: null
      };

      data.forEach(record => {
        const type = record.reading_type;
        stats.byType[type] = (stats.byType[type] || 0) + 1;
        
        if (!stats.latestTimestamp || new Date(record.timestamp) > new Date(stats.latestTimestamp)) {
          stats.latestTimestamp = record.timestamp;
        }
      });

      return {
        success: true,
        source: 'fallback',
        ...stats
      };
    } catch (error) {
      logger.error('Failed to get fallback stats:', error);
      throw error;
    }
  }

  async readParquet(filePath, options = {}) {
    logger.info('Reading Parquet file with DuckDB', { filePath, options });
    
    if (!this.isAvailable || this.dbType !== 'duckdb') {
      throw new Error('DuckDB is required for Parquet file reading');
    }

    try {
      // Use DuckDB's built-in Parquet support
      let sql = `SELECT * FROM read_parquet('${filePath}')`;
      
      // Apply any filters from options
      if (options.where) {
        sql += ` WHERE ${options.where}`;
      }
      
      if (options.orderBy) {
        sql += ` ORDER BY ${options.orderBy}`;
      }
      
      if (options.limit) {
        sql += ` LIMIT ${options.limit}`;
      }

      const results = await this.allDuckDBQuery(sql, []);
      
      logger.info('Successfully read Parquet file', { 
        filePath, 
        recordCount: results.length 
      });
      
      return {
        success: true,
        data: results,
        source: 'duckdb-parquet',
        filePath
      };
      
    } catch (error) {
      logger.error('Failed to read Parquet file with DuckDB:', error);
      throw error;
    }
  }

  async close() {
    if (this.db) {
      if (this.dbType === 'duckdb') {
        return new Promise((resolve) => {
          this.db.close((err) => {
            if (err) {
              logger.error('Error closing DuckDB:', err);
            } else {
              logger.info('DuckDB connection closed');
            }
            this.db = null;
            this.isAvailable = false;
            resolve();
          });
        });
      } else if (this.dbType === 'sqlite') {
        try {
          this.db.close();
          this.db = null;
          this.isAvailable = false;
          logger.info('SQLite connection closed');
        } catch (error) {
          logger.error('Error closing SQLite:', error);
        }
      }
    }
  }

  isUsingDuckDB() {
    return this.isAvailable;
  }

  async updateData(id, updates) {
    // Handle single update by wrapping in array structure
    if (!this.isAvailable) {
      return this.updateDataFallback(id, updates);
    }

    try {
      if (this.dbType === 'sqlite') {
        return await this.updateDataSQLite(id, updates);
      } else if (this.dbType === 'duckdb') {
        return await this.updateDataDuckDB(id, updates);
      }
    } catch (error) {
      logger.error(`Failed to update data in ${this.dbType}:`, error);
      throw error;
    }
  }

  async updateDataDuckDB(id, updates) {
    // First check if record exists
    const checkSQL = `SELECT COUNT(*) as count FROM sensor_data WHERE id = $1`;
    const checkResult = await this.allDuckDBQuery(checkSQL, [id]);
    const exists = checkResult[0] && Number(checkResult[0].count) > 0;
    
    if (!exists) {
      return {
        success: true,
        updated: 0,
        id: id
      };
    }
    
    // Build SET clause dynamically based on updates
    const setClause = Object.keys(updates).map((key, index) => `${key} = $${index + 2}`).join(', ');
    const updateSQL = `UPDATE sensor_data SET ${setClause} WHERE id = $1`;
    
    const params = [id, ...Object.values(updates)];
    
    const result = await this.runDuckDBQuery(updateSQL, params);
    
    return {
      success: true,
      updated: result.changes || 1,
      id: id
    };
  }

  async updateDataSQLite(id, updates) {
    // Build SET clause dynamically based on updates
    const setClause = Object.keys(updates).map(key => `${key} = ?`).join(', ');
    const updateSQL = `UPDATE sensor_data SET ${setClause} WHERE id = ?`;
    
    const params = [...Object.values(updates), id];
    const stmt = this.db.prepare(updateSQL);
    const result = stmt.run(...params);
    
    return {
      success: true,
      updated: result.changes,
      id: id
    };
  }

  async updateDataFallback(id, updates) {
    // For file-based fallback, we would need to read, modify, and write back
    // This is a simplified implementation
    logger.warn('Update operation not fully supported in file-based fallback mode');
    return {
      success: false,
      message: 'Update not supported in fallback mode',
      id: id
    };
  }

  async deleteData(id) {
    if (!this.isAvailable) {
      return this.deleteDataFallback(id);
    }

    try {
      if (this.dbType === 'sqlite') {
        return await this.deleteDataSQLite(id);
      } else if (this.dbType === 'duckdb') {
        return await this.deleteDataDuckDB(id);
      }
    } catch (error) {
      logger.error(`Failed to delete data in ${this.dbType}:`, error);
      throw error;
    }
  }

  async deleteDataDuckDB(id) {
    // First check if record exists
    const checkSQL = `SELECT COUNT(*) as count FROM sensor_data WHERE id = $1`;
    const checkResult = await this.allDuckDBQuery(checkSQL, [id]);
    const exists = checkResult[0] && Number(checkResult[0].count) > 0;
    
    if (!exists) {
      return {
        success: true,
        deleted: 0,
        id: id
      };
    }
    
    const deleteSQL = `DELETE FROM sensor_data WHERE id = $1`;
    const result = await this.runDuckDBQuery(deleteSQL, [id]);
    
    return {
      success: true,
      deleted: result.changes || 1,
      id: id
    };
  }

  async deleteDataSQLite(id) {
    const deleteSQL = `DELETE FROM sensor_data WHERE id = ?`;
    const stmt = this.db.prepare(deleteSQL);
    const result = stmt.run(id);
    
    return {
      success: true,
      deleted: result.changes,
      id: id
    };
  }

  async deleteDataFallback(id) {
    logger.warn('Delete operation not fully supported in file-based fallback mode');
    return {
      success: false,
      message: 'Delete not supported in fallback mode',
      id: id
    };
  }

  // Static method to create test instance with unique paths
  static createTestInstance(testId = Date.now()) {
    const config = {
      paths: {
        processedData: path.join(process.cwd(), 'data', `test-processed-${testId}`)
      }
    };
    
    const instance = new DuckDBService(config);
    
    // Override paths for testing to avoid conflicts
    const dataRoot = path.join(process.cwd(), 'data');
    instance.duckdbPath = path.join(dataRoot, 'database', `agricultural_data_test_${testId}.duckdb`);
    instance.sqlitePath = path.join(dataRoot, 'database', `agricultural_data_test_${testId}.sqlite`);
    
    return instance;
  }
}

module.exports = DuckDBService;
