const moment = require('moment-timezone');

class DateUtils {
  /**
   * Convert timestamp to IST (UTC+5:30)
   */
  static toIST(timestamp) {
    return moment(timestamp).tz('Asia/Kolkata');
  }

  /**
   * Format date for file naming (YYYY-MM-DD)
   */
  static formatDateForFile(date) {
    return moment(date).format('YYYY-MM-DD');
  }

  /**
   * Parse date string to moment object
   */
  static parseDate(dateString) {
    return moment(dateString);
  }

  /**
   * Get date range for processing
   */
  static getDateRange(startDate, endDate) {
    const dates = [];
    const current = moment(startDate);
    const end = moment(endDate);

    while (current.isSameOrBefore(end)) {
      dates.push(current.format('YYYY-MM-DD'));
      current.add(1, 'day');
    }

    return dates;
  }

  /**
   * Check if timestamp is valid ISO format
   */
  static isValidISO(timestamp) {
    return moment(timestamp, moment.ISO_8601, true).isValid();
  }

  /**
   * Convert to ISO string
   */
  static toISOString(timestamp) {
    return moment(timestamp).toISOString();
  }

  /**
   * Get current timestamp in IST
   */
  static nowIST() {
    return moment().tz('Asia/Kolkata');
  }
}

module.exports = DateUtils;
