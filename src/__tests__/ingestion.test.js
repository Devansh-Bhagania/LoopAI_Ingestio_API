const request = require('supertest');
const express = require('express');
const { createIngestion, getStatus } = require('../services/ingestion');
const { initDatabase, clear } = require('../models');

describe('Ingestion API', () => {
  let app;

  beforeEach(async () => {
    // Initialize the database
    await initDatabase();
    
    app = express();
    app.use(express.json());

    // POST /ingest endpoint
    app.post('/ingest', async (req, res) => {
      try {
        const { ids, priority } = req.body;
        
        if (!ids || !Array.isArray(ids) || !priority) {
          return res.status(400).json({ error: 'Invalid request format' });
        }

        const ingestionId = await createIngestion({ ids, priority });
        res.json({ ingestion_id: ingestionId });
      } catch (error) {
        console.error('Error creating ingestion:', error);
        res.status(500).json({ error: 'Internal server error' });
      }
    });

    // GET /status/:ingestionId endpoint
    app.get('/status/:ingestionId', async (req, res) => {
      try {
        const { ingestionId } = req.params;
        const status = await getStatus(ingestionId);
        
        if (!status) {
          return res.status(404).json({ error: 'Ingestion not found' });
        }

        res.json(status);
      } catch (error) {
        console.error('Error getting status:', error);
        res.status(500).json({ error: 'Internal server error' });
      }
    });
  });

  afterEach(async () => {
    // Clear the database after each test
    await clear();
  });

  describe('POST /ingest', () => {
    it('should create a new ingestion request', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'high'
        });

      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('ingestion_id');
    });

    it('should validate request format', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: 'invalid',
          priority: 'high'
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({ error: 'Invalid request format' });
    });
  });

  describe('GET /status/:ingestionId', () => {
    it('should return ingestion status', async () => {
      // First create an ingestion
      const createResponse = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'high'
        });

      const ingestionId = createResponse.body.ingestion_id;

      // Wait for initial processing
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Then get its status
      const statusResponse = await request(app)
        .get(`/status/${ingestionId}`);

      expect(statusResponse.status).toBe(200);
      expect(statusResponse.body).toMatchObject({
        ingestion_id: ingestionId,
        batches: expect.arrayContaining([
          expect.objectContaining({
            batch_id: expect.any(String),
            ids: expect.arrayContaining([1, 2, 3])
          })
        ])
      });

      // Verify the status is either processing or completed
      expect(['processing', 'completed']).toContain(statusResponse.body.status);
    });

    it('should return 404 for non-existent ingestion', async () => {
      const response = await request(app)
        .get('/status/non-existent');

      expect(response.status).toBe(404);
      expect(response.body).toEqual({ error: 'Ingestion not found' });
    });
  });
}); 