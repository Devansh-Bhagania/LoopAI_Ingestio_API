const {
  createIngestion,
  findIngestion,
  createBatch,
  findBatches,
  updateBatch
} = require('../models');

// Simple database operations
const createNewIngestion = async (ingestionId) => {
  try {
    await createIngestion(ingestionId);
    console.log('Created ingestion:', ingestionId);
  } catch (error) {
    console.error('Failed to create ingestion:', error);
    throw error;
  }
};

const createNewBatch = async (batchData) => {
  try {
    await createBatch(batchData);
    console.log('Created batch:', batchData.batch_id);
  } catch (error) {
    console.error('Failed to create batch:', error);
    throw error;
  }
};

const updateBatchStatus = async (batchId, status) => {
  try {
    await updateBatch(batchId, { status });
    console.log('Updated batch status:', { batchId, status });
  } catch (error) {
    console.error('Failed to update batch status:', error);
    throw error;
  }
};

const getIngestionStatus = async (ingestionId) => {
  try {
    const ingestion = await findIngestion(ingestionId);
    
    if (!ingestion) {
      console.log('Ingestion not found:', ingestionId);
      return null;
    }

    const batches = await findBatches({
      where: { ingestion_id: ingestionId },
      order: [['created_at', 'ASC']]
    });

    console.log('Fetched ingestion status:', { ingestion, batches });

    return {
      ingestion_id: ingestionId,
      status: calculateOverallStatus(batches),
      batches: batches.map(batch => ({
        batch_id: batch.batch_id,
        ingestion_id: batch.ingestion_id,
        ids: batch.ids,
        status: batch.status,
        created_at: batch.created_at
      }))
    };
  } catch (error) {
    console.error('Failed to get ingestion status:', error);
    throw error;
  }
};

const calculateOverallStatus = (batches) => {
  if (batches.length === 0) return 'pending';
  if (batches.every(batch => batch.status === 'completed')) {
    return 'completed';
  }
  if (batches.some(batch => batch.status === 'processing')) {
    return 'processing';
  }
  return 'pending';
};

module.exports = {
  createNewIngestion,
  createNewBatch,
  updateBatchStatus,
  getIngestionStatus
}; 