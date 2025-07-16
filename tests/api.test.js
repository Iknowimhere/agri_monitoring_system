const request = require('supertest');
const app = require('../src/app');

describe('Health Check', () => {
  test('GET /health should return health status', async () => {
    const response = await request(app)
      .get('/health')
      .expect(200);

    expect(response.body).toHaveProperty('status', 'healthy');
    expect(response.body).toHaveProperty('timestamp');
    expect(response.body).toHaveProperty('uptime');
    expect(response.body).toHaveProperty('version');
  });
});

describe('API Root', () => {
  test('GET / should return API information', async () => {
    const response = await request(app)
      .get('/')
      .expect(200);

    expect(response.body).toHaveProperty('message', 'Agricultural Data Pipeline API');
    expect(response.body).toHaveProperty('version');
    expect(response.body).toHaveProperty('endpoints');
  });
});

describe('Pipeline API', () => {
  test('GET /api/pipeline/status should return pipeline status', async () => {
    const response = await request(app)
      .get('/api/pipeline/status')
      .expect(200);

    expect(response.body).toHaveProperty('status');
    expect(response.body).toHaveProperty('progress');
  });

  test('POST /api/pipeline/run should start pipeline', async () => {
    const response = await request(app)
      .post('/api/pipeline/run')
      .send({
        startDate: '2023-06-01',
        endDate: '2023-06-03'
      })
      .expect(202);

    expect(response.body).toHaveProperty('message');
    expect(response.body).toHaveProperty('startDate', '2023-06-01');
    expect(response.body).toHaveProperty('endDate', '2023-06-03');
  });

  test('POST /api/pipeline/run should reject invalid date format', async () => {
    const response = await request(app)
      .post('/api/pipeline/run')
      .send({
        startDate: 'invalid-date',
        endDate: '2023-06-03'
      })
      .expect(400);

    expect(response.body).toHaveProperty('error');
  });
});

describe('Data API', () => {
  test('GET /api/data/sensors should return sensors list', async () => {
    const response = await request(app)
      .get('/api/data/sensors')
      .expect(200);

    expect(response.body).toHaveProperty('sensors');
    expect(response.body).toHaveProperty('total_sensors');
  });

  test('GET /api/data/query should return data with pagination', async () => {
    const response = await request(app)
      .get('/api/data/query?page=1&limit=10')
      .expect(200);

    expect(response.body).toHaveProperty('data');
    expect(response.body).toHaveProperty('pagination');
    expect(response.body.pagination).toHaveProperty('page', 1);
    expect(response.body.pagination).toHaveProperty('limit', 10);
  });
});

describe('Reports API', () => {
  test('GET /api/reports/quality should generate quality report', async () => {
    const response = await request(app)
      .get('/api/reports/quality')
      .expect(200);

    expect(response.body).toHaveProperty('report_id');
    expect(response.body).toHaveProperty('generated_at');
    expect(response.body).toHaveProperty('metrics');
  });

  test('GET /api/reports/list should return reports list', async () => {
    const response = await request(app)
      .get('/api/reports/list')
      .expect(200);

    expect(response.body).toHaveProperty('reports');
    expect(response.body).toHaveProperty('total_reports');
  });
});

describe('Error Handling', () => {
  test('404 for non-existent endpoints', async () => {
    const response = await request(app)
      .get('/non-existent-endpoint')
      .expect(404);

    expect(response.body).toHaveProperty('error', 'Endpoint not found');
  });
});
