const express = require('express');
const cors = require('cors');
const { createIngestion, getStatus } = require('./services/ingestion');

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// POST /ingest endpoint
app.post('/ingest', async (req, res) => {
  try {
    const { ids, priority } = req.body;
    
    // Validate request
    if (!ids || !Array.isArray(ids) || !priority) {
      return res.status(400).json({ error: 'Invalid request format' });
    }

    // Validate IDs
    if (!ids.every(id => Number.isInteger(id) && id >= 1 && id <= 1000000007)) {
      return res.status(400).json({ error: 'Invalid ID range' });
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
    console.error('Error getting ingestion status:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
}); 