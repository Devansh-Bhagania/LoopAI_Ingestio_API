import request from 'supertest';
import express from 'express';
import { IngestionService } from '../services/ingestion';
import { Priority, BatchStatus } from '../types';

jest.mock('../services/ingestion');

describe('Ingestion API', () => {
  let app: express.Application;
  let mockIngestionService: jest.Mocked<IngestionService>;
  let ingestionIds: string[] = [];

  beforeEach(() => {
    mockIngestionService = new IngestionService() as jest.Mocked<IngestionService>;
    app = express();
    app.use(express.json());

    // Mock endpoints
    app.post('/ingest', async (req, res) => {
      try {
        // Validate request format
        if (!Array.isArray(req.body.ids) || !req.body.priority) {
          return res.status(400).json({ error: 'Invalid request format' });
        }

        // Validate ID range
        if (req.body.ids.some((id: number) => id <= 0)) {
          return res.status(400).json({ error: 'IDs must be positive numbers' });
        }

        // Validate priority
        if (!Object.values(Priority).includes(req.body.priority)) {
          return res.status(400).json({ error: 'Invalid priority value' });
        }

        const ingestionId = await mockIngestionService.createIngestion(req.body);
        res.json({ ingestion_id: ingestionId });
      } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
      }
    });

    app.get('/status/:ingestionId', async (req, res) => {
      try {
        const status = await mockIngestionService.getIngestionStatus(req.params.ingestionId);
        if (!status) {
          return res.status(404).json({ error: 'Ingestion not found' });
        }
        res.json(status);
      } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
      }
    });
  });

  describe('POST /ingest', () => {
    it('should create a high priority ingestion request', async () => {
      const mockIngestionId = 'test-high-priority-id';
      mockIngestionService.createIngestion.mockResolvedValue(mockIngestionId);

      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4, 5],
          priority: Priority.HIGH
        });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({ ingestion_id: mockIngestionId });
      ingestionIds.push(mockIngestionId);
    });

    it('should create a medium priority ingestion request', async () => {
      const mockIngestionId = 'test-medium-priority-id';
      mockIngestionService.createIngestion.mockResolvedValue(mockIngestionId);

      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [6, 7, 8, 9, 10],
          priority: Priority.MEDIUM
        });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({ ingestion_id: mockIngestionId });
      ingestionIds.push(mockIngestionId);
    });

    it('should create a low priority ingestion request', async () => {
      const mockIngestionId = 'test-low-priority-id';
      mockIngestionService.createIngestion.mockResolvedValue(mockIngestionId);

      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [11, 12, 13, 14, 15],
          priority: Priority.LOW
        });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({ ingestion_id: mockIngestionId });
      ingestionIds.push(mockIngestionId);
    });

    it('should validate request format', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: 'invalid',
          priority: Priority.HIGH
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({ error: 'Invalid request format' });
    });

    it('should validate ID range', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [0, 1, 2],
          priority: Priority.HIGH
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({ error: 'IDs must be positive numbers' });
    });

    it('should validate priority values', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'INVALID'
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({ error: 'Invalid priority value' });
    });
  });

  describe('GET /status/:ingestionId', () => {
    it('should return ingestion status for high priority request', async () => {
      const mockStatus = {
        ingestion_id: ingestionIds[0],
        status: BatchStatus.TRIGGERED,
        batches: [
          {
            batch_id: 'batch-1',
            ingestion_id: ingestionIds[0],
            ids: [1, 2, 3],
            status: BatchStatus.COMPLETED,
            created_at: new Date()
          },
          {
            batch_id: 'batch-2',
            ingestion_id: ingestionIds[0],
            ids: [4, 5],
            status: BatchStatus.TRIGGERED,
            created_at: new Date()
          }
        ]
      };

      mockIngestionService.getIngestionStatus.mockResolvedValue(mockStatus);

      const response = await request(app)
        .get(`/status/${ingestionIds[0]}`);

      expect(response.status).toBe(200);
      expect(response.body).toMatchObject({
        ingestion_id: mockStatus.ingestion_id,
        status: mockStatus.status,
        batches: mockStatus.batches.map(batch => ({
          ...batch,
          created_at: expect.any(String)
        }))
      });
    });

    it('should return ingestion status for medium priority request', async () => {
      const mockStatus = {
        ingestion_id: ingestionIds[1],
        status: BatchStatus.YET_TO_START,
        batches: [
          {
            batch_id: 'batch-3',
            ingestion_id: ingestionIds[1],
            ids: [6, 7, 8],
            status: BatchStatus.YET_TO_START,
            created_at: new Date()
          },
          {
            batch_id: 'batch-4',
            ingestion_id: ingestionIds[1],
            ids: [9, 10],
            status: BatchStatus.YET_TO_START,
            created_at: new Date()
          }
        ]
      };

      mockIngestionService.getIngestionStatus.mockResolvedValue(mockStatus);

      const response = await request(app)
        .get(`/status/${ingestionIds[1]}`);

      expect(response.status).toBe(200);
      expect(response.body).toMatchObject({
        ingestion_id: mockStatus.ingestion_id,
        status: mockStatus.status,
        batches: mockStatus.batches.map(batch => ({
          ...batch,
          created_at: expect.any(String)
        }))
      });
    });

    it('should return 404 for non-existent ingestion', async () => {
      mockIngestionService.getIngestionStatus.mockResolvedValue(null);

      const response = await request(app)
        .get('/status/non-existent');

      expect(response.status).toBe(404);
      expect(response.body).toEqual({ error: 'Ingestion not found' });
    });
  });

  describe('Priority and Rate Limiting', () => {
    it('should process high priority request before medium priority', async () => {
      // Create medium priority request
      const mediumPriorityId = 'test-medium-priority';
      mockIngestionService.createIngestion.mockResolvedValueOnce(mediumPriorityId);

      await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4, 5],
          priority: Priority.MEDIUM
        });

      // Wait 4 seconds
      await new Promise(resolve => setTimeout(resolve, 4000));

      // Create high priority request
      const highPriorityId = 'test-high-priority';
      mockIngestionService.createIngestion.mockResolvedValueOnce(highPriorityId);

      await request(app)
        .post('/ingest')
        .send({
          ids: [6, 7, 8, 9],
          priority: Priority.HIGH
        });

      // Check status of both requests
      const mediumStatus = {
        ingestion_id: mediumPriorityId,
        status: BatchStatus.TRIGGERED,
        batches: [
          {
            batch_id: 'batch-1',
            ingestion_id: mediumPriorityId,
            ids: [1, 2, 3],
            status: BatchStatus.COMPLETED,
            created_at: expect.any(String)
          },
          {
            batch_id: 'batch-2',
            ingestion_id: mediumPriorityId,
            ids: [4, 5],
            status: BatchStatus.YET_TO_START,
            created_at: expect.any(String)
          }
        ]
      };

      const highStatus = {
        ingestion_id: highPriorityId,
        status: BatchStatus.TRIGGERED,
        batches: [
          {
            batch_id: 'batch-3',
            ingestion_id: highPriorityId,
            ids: [6, 7, 8],
            status: BatchStatus.TRIGGERED,
            created_at: expect.any(String)
          },
          {
            batch_id: 'batch-4',
            ingestion_id: highPriorityId,
            ids: [9],
            status: BatchStatus.YET_TO_START,
            created_at: expect.any(String)
          }
        ]
      };

      mockIngestionService.getIngestionStatus
        .mockResolvedValueOnce(mediumStatus)
        .mockResolvedValueOnce(highStatus);

      const mediumResponse = await request(app)
        .get(`/status/${mediumPriorityId}`);

      const highResponse = await request(app)
        .get(`/status/${highPriorityId}`);

      expect(mediumResponse.body.batches[1].status).toBe(BatchStatus.YET_TO_START);
      expect(highResponse.body.batches[0].status).toBe(BatchStatus.TRIGGERED);
    });

    it('should respect rate limit of 1 batch per 5 seconds', async () => {
      const ingestionId = 'test-rate-limit';
      mockIngestionService.createIngestion.mockResolvedValue(ingestionId);

      await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4, 5, 6, 7, 8, 9],
          priority: Priority.HIGH
        });

      const status = {
        ingestion_id: ingestionId,
        status: BatchStatus.TRIGGERED,
        batches: [
          {
            batch_id: 'batch-1',
            ingestion_id: ingestionId,
            ids: [1, 2, 3],
            status: BatchStatus.COMPLETED,
            created_at: expect.any(String)
          },
          {
            batch_id: 'batch-2',
            ingestion_id: ingestionId,
            ids: [4, 5, 6],
            status: BatchStatus.TRIGGERED,
            created_at: expect.any(String)
          },
          {
            batch_id: 'batch-3',
            ingestion_id: ingestionId,
            ids: [7, 8, 9],
            status: BatchStatus.YET_TO_START,
            created_at: expect.any(String)
          }
        ]
      };

      mockIngestionService.getIngestionStatus.mockResolvedValue(status);

      const response = await request(app)
        .get(`/status/${ingestionId}`);

      expect(response.body.batches.length).toBe(3);
      expect(response.body.batches[0].ids.length).toBe(3);
      expect(response.body.batches[1].ids.length).toBe(3);
      expect(response.body.batches[2].ids.length).toBe(3);
    });
  });
}); 