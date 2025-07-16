const logger = require('./logger');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

class BuildToolsChecker {
  constructor() {
    this.checkedTools = false;
    this.toolsAvailable = false;
    this.duckdbAvailable = false;
  }

  async checkCppBuildTools() {
    if (this.checkedTools) {
      return this.toolsAvailable;
    }

    try {
      logger.info('Checking C++ build tools availability...');
      
      const checks = await Promise.allSettled([
        this.checkNodeGyp(),
        this.checkMSVS(),
        this.checkPython()
      ]);

      const results = checks.map(check => check.status === 'fulfilled' && check.value);
      this.toolsAvailable = results.some(result => result === true);
      this.checkedTools = true;

      logger.info('Build tools check completed', {
        nodeGyp: results[0],
        msvcBuildTools: results[1],
        python: results[2],
        available: this.toolsAvailable
      });

      return this.toolsAvailable;
    } catch (error) {
      logger.error('Failed to check build tools:', error);
      this.toolsAvailable = false;
      this.checkedTools = true;
      return false;
    }
  }

  async checkNodeGyp() {
    try {
      await execAsync('node-gyp --version', { timeout: 5000 });
      return true;
    } catch (error) {
      return false;
    }
  }

  async checkMSVS() {
    try {
      // Check for Visual Studio Build Tools
      const { stdout } = await execAsync('where cl', { timeout: 5000 });
      return stdout.includes('cl.exe');
    } catch (error) {
      return false;
    }
  }

  async checkPython() {
    try {
      const { stdout } = await execAsync('python --version', { timeout: 5000 });
      return stdout.includes('Python');
    } catch (error) {
      try {
        const { stdout } = await execAsync('python3 --version', { timeout: 5000 });
        return stdout.includes('Python');
      } catch (error2) {
        return false;
      }
    }
  }

  async checkDuckDB() {
    if (this.duckdbAvailable !== null) {
      return this.duckdbAvailable;
    }

    try {
      logger.info('Testing DuckDB availability...');
      
      // Test DuckDB loading
      const duckdb = require('duckdb');
      
      // Test database creation
      return new Promise((resolve) => {
        const db = new duckdb.Database(':memory:', (err) => {
          if (err) {
            logger.warn('DuckDB test failed:', err.message);
            this.duckdbAvailable = false;
            resolve(false);
          } else {
            logger.info('✅ DuckDB is working correctly');
            this.duckdbAvailable = true;
            db.close();
            resolve(true);
          }
        });
      });
    } catch (error) {
      logger.warn('DuckDB is not available:', error.message);
      this.duckdbAvailable = false;
      return false;
    }
  }

  async getRecommendations() {
    const buildToolsOk = await this.checkCppBuildTools();
    const duckdbOk = await this.checkDuckDB();

    const recommendations = [];

    if (!buildToolsOk) {
      recommendations.push(
        'Install Visual Studio Build Tools: npm install --global windows-build-tools',
        'Or install Visual Studio Community with C++ workload',
        'Install Python 3.x: https://python.org'
      );
    }

    if (!duckdbOk && buildToolsOk) {
      recommendations.push(
        'Reinstall DuckDB: npm uninstall duckdb && npm install duckdb',
        'Try alternative: npm install duckdb-node'
      );
    }

    if (!duckdbOk && !buildToolsOk) {
      recommendations.push(
        'Consider using SQLite as alternative: npm install sqlite3',
        'Or continue with file-based storage for now'
      );
    }

    return {
      buildTools: buildToolsOk,
      duckdb: duckdbOk,
      recommendations
    };
  }
}

module.exports = new BuildToolsChecker();
