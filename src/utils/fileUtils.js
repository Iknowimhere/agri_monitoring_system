const fs = require('fs-extra');
const path = require('path');
const logger = require('./logger');

class FileUtils {
  /**
   * Ensure directory exists
   */
  static async ensureDir(dirPath) {
    try {
      await fs.ensureDir(dirPath);
      return true;
    } catch (error) {
      logger.error(`Failed to create directory ${dirPath}:`, error);
      throw error;
    }
  }

  /**
   * Check if file exists
   */
  static async fileExists(filePath) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get file size in MB
   */
  static async getFileSizeMB(filePath) {
    try {
      const stats = await fs.stat(filePath);
      return stats.size / (1024 * 1024);
    } catch (error) {
      logger.error(`Failed to get file size for ${filePath}:`, error);
      return 0;
    }
  }

  /**
   * List files in directory with pattern
   */
  static async listFiles(dirPath, pattern = '*') {
    try {
      const files = await fs.readdir(dirPath);
      
      if (pattern === '*') {
        return files.map(file => path.join(dirPath, file));
      }
      
      // Simple pattern matching for .parquet files
      const regex = new RegExp(pattern.replace('*', '.*'));
      return files
        .filter(file => regex.test(file))
        .map(file => path.join(dirPath, file));
    } catch (error) {
      logger.error(`Failed to list files in ${dirPath}:`, error);
      return [];
    }
  }

  /**
   * Copy file
   */
  static async copyFile(source, destination) {
    try {
      await fs.copy(source, destination);
      logger.info(`File copied from ${source} to ${destination}`);
      return true;
    } catch (error) {
      logger.error(`Failed to copy file from ${source} to ${destination}:`, error);
      throw error;
    }
  }

  /**
   * Delete file
   */
  static async deleteFile(filePath) {
    try {
      await fs.remove(filePath);
      logger.info(`File deleted: ${filePath}`);
      return true;
    } catch (error) {
      logger.error(`Failed to delete file ${filePath}:`, error);
      throw error;
    }
  }

  /**
   * Read JSON file
   */
  static async readJSON(filePath) {
    try {
      return await fs.readJSON(filePath);
    } catch (error) {
      logger.error(`Failed to read JSON file ${filePath}:`, error);
      throw error;
    }
  }

  /**
   * Write JSON file
   */
  static async writeJSON(filePath, data) {
    try {
      await fs.writeJSON(filePath, data, { spaces: 2 });
      logger.info(`JSON file written: ${filePath}`);
      return true;
    } catch (error) {
      logger.error(`Failed to write JSON file ${filePath}:`, error);
      throw error;
    }
  }

  /**
   * Get file extension
   */
  static getExtension(filePath) {
    return path.extname(filePath).toLowerCase();
  }

  /**
   * Generate unique filename
   */
  static generateUniqueFilename(baseName, extension) {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 8);
    return `${baseName}_${timestamp}_${random}${extension}`;
  }
}

module.exports = FileUtils;
