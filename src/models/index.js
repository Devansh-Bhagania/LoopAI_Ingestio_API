// Simple in-memory storage
const ingestions = new Map();
const batches = new Map();

const createNewIngestion = async (ingestionId) => {
  const ingestion = {
    ingestion_id: ingestionId,
    status: 'pending',
    created_at: new Date()
  };
  ingestions.set(ingestionId, ingestion);
  return ingestion;
};

const findIngestion = async (ingestionId) => {
  return ingestions.get(ingestionId) || null;
};

const createNewBatch = async (batchData) => {
  const batch = {
    batch_id: batchData.batch_id,
    ingestion_id: batchData.ingestion_id,
    ids: batchData.ids,
    status: batchData.status || 'pending',
    created_at: new Date()
  };
  batches.set(batchData.batch_id, batch);
  return batch;
};

const findBatches = async (options) => {
  let results = Array.from(batches.values());
  if (options.where && options.where.ingestion_id) {
    results = results.filter(batch => batch.ingestion_id === options.where.ingestion_id);
  }
  if (options.order) {
    const [field, direction] = options.order[0];
    results.sort((a, b) => {
      const aValue = a[field];
      const bValue = b[field];
      if (direction === 'ASC') {
        return aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
      }
      return aValue > bValue ? -1 : aValue < bValue ? 1 : 0;
    });
  }
  return results;
};

const updateBatch = async (batchId, data) => {
  const batch = batches.get(batchId);
  if (batch) {
    Object.assign(batch, data);
    batches.set(batchId, batch);
  }
  return batch || null;
};

const updateBatchStatus = async (batchId, status) => {
  return updateBatch(batchId, { status });
};

const getIngestionStatus = async (ingestionId) => {
  const ingestion = await findIngestion(ingestionId);
  if (!ingestion) {
    return null;
  }
  const batchList = await findBatches({
    where: { ingestion_id: ingestionId },
    order: [['created_at', 'ASC']]
  });
  return {
    ingestion_id: ingestionId,
    status: calculateOverallStatus(batchList),
    batches: batchList
  };
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

const initDatabase = async () => {
  try {
    console.log('In-memory database initialized successfully');
    return true;
  } catch (error) {
    console.error('Failed to initialize in-memory database:', error);
    throw error;
  }
};

module.exports = {
  createNewIngestion,
  findIngestion,
  createNewBatch,
  findBatches,
  updateBatch,
  updateBatchStatus,
  getIngestionStatus,
  initDatabase
}; 