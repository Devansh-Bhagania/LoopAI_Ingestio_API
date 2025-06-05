const {
  createNewIngestion,
  createNewBatch,
  updateBatchStatus,
  getIngestionStatus
} = require('./database');

// Simple ingestion operations
const createIngestion = async (request) => {
  const ingestionId = Date.now().toString();
  
  // Create ingestion
  await createNewIngestion(ingestionId);

  // Split IDs into batches of 3
  const batches = [];
  for (let i = 0; i < request.ids.length; i += 3) {
    const batchIds = request.ids.slice(i, i + 3);
    const batchId = `${ingestionId}-batch-${batches.length}`;
    
    const batch = {
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
  processBatches(batches, request.priority);

  return ingestionId;
};

const getStatus = async (ingestionId) => {
  return getIngestionStatus(ingestionId);
};

const processBatches = async (batches, priority) => {
  for (const batch of batches) {
    // Simulate processing delay based on priority
    const delay = priority === 'high' ? 1000 : priority === 'medium' ? 2000 : 3000;
    await new Promise(resolve => setTimeout(resolve, delay));
    
    // Update batch status
    await updateBatchStatus(batch.batch_id, 'completed');
  }
};

module.exports = {
  createIngestion,
  getStatus
}; 