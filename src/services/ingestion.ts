import { v4 as uuidv4 } from 'uuid';
import { Priority, BatchStatus, IngestionRequest, IngestionStatus, Batch } from '../types';
import { Ingestion, Batch as BatchModel } from '../models';

export class IngestionService {
  private processingQueue: { ingestionId: string; priority: Priority; createdAt: Date }[] = [];
  private isProcessing: boolean = false;
  private lastProcessedTime: number = 0;
  private readonly RATE_LIMIT_INTERVAL: number = 5000; // 5 seconds in milliseconds
  private readonly BATCH_SIZE: number = 3; // Process 3 IDs per batch

  async createIngestion(request: IngestionRequest): Promise<string> {
    const ingestionId = uuidv4();
    const ingestion = await Ingestion.create({
      ingestion_id: ingestionId,
      priority: request.priority,
      status: BatchStatus.YET_TO_START
    });

    // Split IDs into batches of 3
    const batches = this.splitIntoBatches(request.ids, this.BATCH_SIZE);
    for (const batchIds of batches) {
      await BatchModel.create({
        batch_id: uuidv4(),
        ingestion_id: ingestionId,
        ids: batchIds,
        status: BatchStatus.YET_TO_START
      });
    }

    // Add to processing queue
    this.processingQueue.push({
      ingestionId,
      priority: request.priority,
      createdAt: new Date()
    });

    // Start processing if not already running
    if (!this.isProcessing) {
      this.processQueue();
    }

    return ingestionId;
  }

  private splitIntoBatches(ids: number[], batchSize: number): number[][] {
    const batches: number[][] = [];
    for (let i = 0; i < ids.length; i += batchSize) {
      batches.push(ids.slice(i, i + batchSize));
    }
    return batches;
  }

  private async processQueue() {
    if (this.isProcessing || this.processingQueue.length === 0) return;

    this.isProcessing = true;
    while (this.processingQueue.length > 0) {
      const now = Date.now();
      const timeSinceLastProcessed = now - this.lastProcessedTime;

      if (timeSinceLastProcessed < this.RATE_LIMIT_INTERVAL) {
        await new Promise(resolve => setTimeout(resolve, this.RATE_LIMIT_INTERVAL - timeSinceLastProcessed));
      }

      // Sort queue by priority (HIGH > MEDIUM > LOW) and creation time
      this.processingQueue.sort((a, b) => {
        const priorityOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
        if (priorityOrder[a.priority] !== priorityOrder[b.priority]) {
          return priorityOrder[a.priority] - priorityOrder[b.priority];
        }
        return a.createdAt.getTime() - b.createdAt.getTime();
      });

      const { ingestionId } = this.processingQueue.shift()!;
      const batches = await BatchModel.findAll({ where: { ingestion_id: ingestionId } });

      for (const batch of batches) {
        if (batch.status === BatchStatus.YET_TO_START) {
          await this.processBatch(batch);
          this.lastProcessedTime = Date.now();
        }
      }
    }
    this.isProcessing = false;
  }

  private async processBatch(batch: BatchModel) {
    batch.status = BatchStatus.TRIGGERED;
    await batch.save();

    // Simulate external API call
    await new Promise(resolve => setTimeout(resolve, 1000));

    batch.status = BatchStatus.COMPLETED;
    await batch.save();
  }

  async getIngestionStatus(ingestionId: string): Promise<IngestionStatus | null> {
    const ingestion = await Ingestion.findByPk(ingestionId);
    if (!ingestion) return null;

    const batches = await BatchModel.findAll({ where: { ingestion_id: ingestionId } });
    const batchStatuses = batches.map(batch => ({
      batch_id: batch.batch_id,
      ingestion_id: batch.ingestion_id,
      ids: batch.ids,
      status: batch.status,
      created_at: batch.created_at
    }));

    const overallStatus = this.calculateOverallStatus(batchStatuses);

    return {
      ingestion_id: ingestionId,
      status: overallStatus,
      batches: batchStatuses
    };
  }

  private calculateOverallStatus(batches: Batch[]): BatchStatus {
    if (batches.every(batch => batch.status === BatchStatus.COMPLETED)) {
      return BatchStatus.COMPLETED;
    } else if (batches.some(batch => batch.status === BatchStatus.TRIGGERED)) {
      return BatchStatus.TRIGGERED;
    } else {
      return BatchStatus.YET_TO_START;
    }
  }
} 