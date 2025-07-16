const fs = require('fs');
const path = require('path');

// Mock data generation for testing
function generateSampleData() {
  const sensors = ['SENSOR_001', 'SENSOR_002', 'SENSOR_003'];
  const readingTypes = ['temperature', 'humidity', 'soil_moisture', 'light_intensity', 'battery_level'];
  const data = [];

  const startDate = new Date('2023-06-01');
  const endDate = new Date('2023-06-03');

  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    for (let hour = 0; hour < 24; hour++) {
      for (const sensor of sensors) {
        for (const readingType of readingTypes) {
          const timestamp = new Date(d);
          timestamp.setHours(hour, Math.floor(Math.random() * 60), Math.floor(Math.random() * 60));

          let value;
          switch (readingType) {
            case 'temperature':
              value = 15 + Math.random() * 30; // 15-45°C
              break;
            case 'humidity':
              value = 30 + Math.random() * 60; // 30-90%
              break;
            case 'soil_moisture':
              value = 20 + Math.random() * 70; // 20-90%
              break;
            case 'light_intensity':
              value = Math.random() * 80000; // 0-80000 lux
              break;
            case 'battery_level':
              value = 60 + Math.random() * 40; // 60-100%
              break;
          }

          // Add some anomalies (5% chance)
          if (Math.random() < 0.05) {
            value *= 2; // Make it anomalous
          }

          data.push({
            sensor_id: sensor,
            timestamp: timestamp.toISOString(),
            reading_type: readingType,
            value: Math.round(value * 100) / 100,
            battery_level: 60 + Math.random() * 40
          });
        }
      }
    }
  }

  return data;
}

// This would normally be generated using a proper Parquet library
// For demonstration, we'll create JSON files that can be processed
async function createSampleFiles() {
  const dataDir = path.join(__dirname, '../../../data/raw');
  
  // Ensure directory exists
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const dates = ['2023-06-01', '2023-06-02', '2023-06-03'];
  
  for (const date of dates) {
    const dayData = generateSampleData().filter(record => 
      record.timestamp.startsWith(date)
    );
    
    const fileName = `${date}.json`; // Using JSON instead of Parquet for simplicity
    const filePath = path.join(dataDir, fileName);
    
    fs.writeFileSync(filePath, JSON.stringify(dayData, null, 2));
    console.log(`Created sample file: ${fileName} with ${dayData.length} records`);
  }
}

module.exports = { generateSampleData, createSampleFiles };
