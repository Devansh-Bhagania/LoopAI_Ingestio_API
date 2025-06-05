import { Batch, BatchStatus, IngestionStatus } from '../types';
import { Ingestion, Batch as BatchModel } from '../models';

export class DatabaseService {
  async createIngestion(ingestionId: string): Promise<void> {
    try {
      await Ingestion.create({
        ingestion_id: ingestionId,
        status: BatchStatus.YET_TO_START
      });
      console.log('Created ingestion:', ingestionId);
    } catch (error) {
      console.error('Failed to create ingestion:', error);
      throw error;
    }
  }

  async createBatch(batch: Batch): Promise<void> {
    try {
      await BatchModel.create({
        batch_id: batch.batch_id,
        ingestion_id: batch.ingestion_id,
        ids: batch.ids,
        status: batch.status,
        created_at: batch.created_at
      });
      console.log('Created batch:', batch.batch_id);
    } catch (error) {
      console.error('Failed to create batch:', error);
      throw error;
    }
  }

  async updateBatchStatus(batchId: string, status: BatchStatus): Promise<void> {
    try {
      await BatchModel.update(
        { status },
        { where: { batch_id: batchId } }
      );
      console.log('Updated batch status:', { batchId, status });
    } catch (error) {
      console.error('Failed to update batch status:', error);
      throw error;
    }
  }

  async getIngestionStatus(ingestionId: string): Promise<IngestionStatus | null> {
    try {
      const ingestion = await Ingestion.findByPk(ingestionId);
      
      if (!ingestion) {
        console.log('Ingestion not found:', ingestionId);
        return null;
      }

      const batches = await BatchModel.findAll({
        where: { ingestion_id: ingestionId },
        order: [['created_at', 'ASC']]
      });

      console.log('Fetched ingestion status:', { ingestion, batches });

      return {
        ingestion_id: ingestionId,
        status: this.calculateOverallStatus(batches),
        batches: batches.map(batch => ({
          batch_id: batch.batch_id,
          ingestion_id: batch.ingestion_id,
          ids: batch.ids,
          status: batch.status as BatchStatus,
          created_at: batch.created_at
        }))
      };
    } catch (error) {
      console.error('Failed to get ingestion status:', error);
      throw error;
    }
  }

  private calculateOverallStatus(batches: BatchModel[]): BatchStatus {
    if (batches.length === 0) return BatchStatus.YET_TO_START;
    if (batches.every(batch => batch.status === BatchStatus.COMPLETED)) {
      return BatchStatus.COMPLETED;
    }
    if (batches.some(batch => batch.status === BatchStatus.TRIGGERED)) {
      return BatchStatus.TRIGGERED;
    }
    return BatchStatus.YET_TO_START;
  }
} 