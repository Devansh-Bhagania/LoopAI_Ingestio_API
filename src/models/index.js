// Simple in-memory storage
const ingestions = new Map();
const batches = new Map();

// Simple functions for ingestion operations
const createIngestion = async (ingestionId) => {
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

// Simple functions for batch operations
const createBatch = async (batchData) => {
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
    results = results.filter(batch => 
      batch.ingestion_id === options.where.ingestion_id
    );
  }

  if (options.order) {
    const [field, direction] = options.order[0];
    results.sort((a, b) => {
      if (direction === 'ASC') {
        return a[field] - b[field];
      }
      return b[field] - a[field];
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
  return batch;
};

// Simple database initialization
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
  createIngestion,
  findIngestion,
  createBatch,
  findBatches,
  updateBatch,
  initDatabase
}; 