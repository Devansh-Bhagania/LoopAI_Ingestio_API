import { v4 as uuidv4 } from 'uuid';
import { Priority, BatchStatus, IngestionRequest, IngestionStatus, Batch } from '../types';
import { Ingestion, Batch as BatchModel } from '../models';
import {
  createNewIngestion,
  createNewBatch,
  updateBatchStatus,
  getIngestionStatus
} from '../models';

export class IngestionService {
  private processingQueue: { ingestionId: string; priority: Priority; createdAt: Date }[] = [];
  private isProcessing: boolean = false;
  private lastProcessedTime: number = 0;
  private readonly RATE_LIMIT_INTERVAL: number = 5000; // 5 seconds in milliseconds
  private readonly BATCH_SIZE: number = 3; // Process 3 IDs per batch

  async createIngestion(request: IngestionRequest): Promise<string> {
    const ingestionId = Date.now().toString();
    
    // Create ingestion
    await createNewIngestion(ingestionId);

    // Split IDs into batches of 3
    const batches: Batch[] = [];
    for (let i = 0; i < request.ids.length; i += 3) {
      const batchIds = request.ids.slice(i, i + 3);
      const batchId = `${ingestionId}-batch-${batches.length}`;
      
      const batch: Batch = {
        batch_id: batchId,
        ingestion_id: ingestionId,
        ids: batchIds,
        status: 'pending',
        created_at: new Date()
      };
      
      await createNewBatch(batch);
      batches.push(batch);
    }

    // Process batches
    await this.processBatches(batches, request.priority);

    return ingestionId;
  }

  private async processBatches(batches: Batch[], priority: 'high' | 'medium' | 'low'): Promise<void> {
    for (const batch of batches) {
      // Simulate processing delay based on priority
      const delay = priority === 'high' ? 1000 : priority === 'medium' ? 2000 : 3000;
      await new Promise(resolve => setTimeout(resolve, delay));
      
      // Update batch status
      await updateBatchStatus(batch.batch_id, 'completed');
    }
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

  async getStatus(ingestionId: string) {
    return getIngestionStatus(ingestionId);
  }
} 