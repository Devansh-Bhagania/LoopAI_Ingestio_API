// In-memory database with enhanced features
class InMemoryDB {
  constructor() {
    this.ingestions = new Map();
    this.batches = new Map();
    this.isInitialized = false;
    this.processingQueue = [];
    this.isProcessing = false;
    this.lastProcessedTime = 0;
    this.RATE_LIMIT_INTERVAL = 5000; // 5 seconds in milliseconds
  }

  // Initialize the database
  async init() {
    try {
      this.isInitialized = true;
      console.log('In-memory database initialized successfully');
      return true;
    } catch (error) {
      console.error('Failed to initialize in-memory database:', error);
      throw new Error('Database initialization failed');
    }
  }

  // Validate ingestion data
  validateIngestion(ingestion) {
    if (!ingestion.ingestion_id || typeof ingestion.ingestion_id !== 'string') {
      throw new Error('Invalid ingestion_id');
    }
    if (!['pending', 'processing', 'completed'].includes(ingestion.status)) {
      throw new Error('Invalid status');
    }
    if (!(ingestion.created_at instanceof Date)) {
      throw new Error('Invalid created_at date');
    }
  }

  // Validate batch data
  validateBatch(batch) {
    if (!batch.batch_id || typeof batch.batch_id !== 'string') {
      throw new Error('Invalid batch_id');
    }
    if (!batch.ingestion_id || typeof batch.ingestion_id !== 'string') {
      throw new Error('Invalid ingestion_id');
    }
    if (!Array.isArray(batch.ids) || !batch.ids.every(id => Number.isInteger(id))) {
      throw new Error('Invalid ids array');
    }
    if (!['pending', 'processing', 'completed'].includes(batch.status)) {
      throw new Error('Invalid status');
    }
    if (!(batch.created_at instanceof Date)) {
      throw new Error('Invalid created_at date');
    }
  }

  // Create a new ingestion
  async createNewIngestion(ingestionId) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    if (this.ingestions.has(ingestionId)) {
      throw new Error('Ingestion already exists');
    }

    const ingestion = {
      ingestion_id: ingestionId,
      status: 'pending',
      created_at: new Date(),
      updated_at: new Date()
    };

    this.validateIngestion(ingestion);
    this.ingestions.set(ingestionId, ingestion);
    return ingestion;
  }

  // Find an ingestion by ID
  async findIngestion(ingestionId) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    return this.ingestions.get(ingestionId) || null;
  }

  // Create a new batch
  async createNewBatch(batchData) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    if (this.batches.has(batchData.batch_id)) {
      throw new Error('Batch already exists');
    }

    // Verify that the ingestion exists
    const ingestion = await this.findIngestion(batchData.ingestion_id);
    if (!ingestion) {
      throw new Error('Ingestion not found');
    }

    const batch = {
      batch_id: batchData.batch_id,
      ingestion_id: batchData.ingestion_id,
      ids: batchData.ids,
      status: batchData.status || 'pending',
      created_at: new Date(),
      updated_at: new Date()
    };

    this.validateBatch(batch);
    this.batches.set(batchData.batch_id, batch);

    // Add to processing queue
    this.processingQueue.push({
      batchId: batch.batch_id,
      priority: batchData.priority || 'medium',
      createdAt: new Date()
    });

    // Start processing if not already running
    if (!this.isProcessing) {
      this.processQueue();
    }

    return batch;
  }

  // Process the queue with rate limiting
  async processQueue() {
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
        const priorityOrder = { high: 0, medium: 1, low: 2 };
        if (priorityOrder[a.priority] !== priorityOrder[b.priority]) {
          return priorityOrder[a.priority] - priorityOrder[b.priority];
        }
        return a.createdAt.getTime() - b.createdAt.getTime();
      });

      const { batchId } = this.processingQueue.shift();
      const batch = this.batches.get(batchId);

      if (batch && batch.status === 'pending') {
        // Update batch status to processing
        await this.updateBatchStatus(batchId, 'processing');
        
        // Simulate processing delay based on priority
        const delay = batch.priority === 'high' ? 1000 : batch.priority === 'medium' ? 2000 : 3000;
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Update batch status to completed
        await this.updateBatchStatus(batchId, 'completed');
        this.lastProcessedTime = Date.now();
      }
    }
    this.isProcessing = false;
  }

  // Find batches with filtering and sorting
  async findBatches(options = {}) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    let results = Array.from(this.batches.values());

    // Apply filters
    if (options.where) {
      if (options.where.ingestion_id) {
        results = results.filter(batch => batch.ingestion_id === options.where.ingestion_id);
      }
      if (options.where.status) {
        results = results.filter(batch => batch.status === options.where.status);
      }
    }

    // Apply sorting
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

    // Apply pagination
    if (options.limit) {
      const offset = options.offset || 0;
      results = results.slice(offset, offset + options.limit);
    }

    return results;
  }

  // Update a batch
  async updateBatch(batchId, data) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    const batch = this.batches.get(batchId);
    if (!batch) {
      throw new Error('Batch not found');
    }

    const updatedBatch = {
      ...batch,
      ...data,
      updated_at: new Date()
    };

    this.validateBatch(updatedBatch);
    this.batches.set(batchId, updatedBatch);
    return updatedBatch;
  }

  // Update batch status
  async updateBatchStatus(batchId, status) {
    if (!['pending', 'processing', 'completed'].includes(status)) {
      throw new Error('Invalid status');
    }
    return this.updateBatch(batchId, { status });
  }

  // Get ingestion status with all related batches
  async getIngestionStatus(ingestionId) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    const ingestion = await this.findIngestion(ingestionId);
    if (!ingestion) {
      return null;
    }

    const batches = await this.findBatches({
      where: { ingestion_id: ingestionId },
      order: [['created_at', 'ASC']]
    });

    return {
      ingestion_id: ingestionId,
      status: this.calculateOverallStatus(batches),
      batches,
      created_at: ingestion.created_at,
      updated_at: ingestion.updated_at
    };
  }

  // Calculate overall status based on batch statuses
  calculateOverallStatus(batches) {
    if (batches.length === 0) return 'pending';
    if (batches.every(batch => batch.status === 'completed')) {
      return 'completed';
    }
    if (batches.some(batch => batch.status === 'processing')) {
      return 'processing';
    }
    return 'pending';
  }

  // Get database statistics
  async getStats() {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    const totalIngestions = this.ingestions.size;
    const totalBatches = this.batches.size;
    
    const statusCounts = {
      pending: 0,
      processing: 0,
      completed: 0
    };

    for (const batch of this.batches.values()) {
      statusCounts[batch.status]++;
    }

    return {
      total_ingestions: totalIngestions,
      total_batches: totalBatches,
      status_counts: statusCounts,
      queue_length: this.processingQueue.length,
      is_processing: this.isProcessing
    };
  }

  // Clear all data (useful for testing)
  async clear() {
    if (!this.isInitialized) {
      throw new Error('Database not initialized');
    }

    this.ingestions.clear();
    this.batches.clear();
    this.processingQueue = [];
    this.isProcessing = false;
    this.lastProcessedTime = 0;
    return true;
  }
}

// Create a singleton instance
const db = new InMemoryDB();

// Export the database functions
module.exports = {
  createNewIngestion: (...args) => db.createNewIngestion(...args),
  findIngestion: (...args) => db.findIngestion(...args),
  createNewBatch: (...args) => db.createNewBatch(...args),
  findBatches: (...args) => db.findBatches(...args),
  updateBatch: (...args) => db.updateBatch(...args),
  updateBatchStatus: (...args) => db.updateBatchStatus(...args),
  getIngestionStatus: (...args) => db.getIngestionStatus(...args),
  initDatabase: () => db.init(),
  getStats: () => db.getStats(),
  clear: () => db.clear()
}; 