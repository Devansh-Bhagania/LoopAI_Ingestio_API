# Data Ingestion API System

A robust data ingestion system built with Express.js and in-memory storage, featuring rate limiting, priority-based processing, and comprehensive status tracking.

## Features

- **Rate Limited Processing**: Processes exactly 1 batch every 5 seconds
- **Priority-Based Processing**: Supports HIGH, MEDIUM, and LOW priority levels
- **In-Memory Storage**: Fast and efficient data storage with validation
- **Comprehensive Status Tracking**: Detailed status tracking for both ingestions and batches
- **Queue Management**: Automatic queue processing with priority sorting
- **Health Monitoring**: Built-in health check endpoint
- **Error Handling**: Robust error handling and validation
- **Statistics**: Real-time system statistics and queue monitoring

## API Endpoints

### 1. Create Ingestion
```http
POST /ingestion
Content-Type: application/json

{
  "ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  "priority": "high"  // Optional: "high", "medium", or "low"
}
```

Response:
```json
{
  "ingestion_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2024-03-14T12:00:00.000Z"
}
```

### 2. Get Ingestion Status
```http
GET /ingestion/{ingestion_id}
```

Response:
```json
{
  "ingestion_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "batches": [
    {
      "batch_id": "batch-1",
      "status": "completed",
      "ids": [1, 2, 3],
      "created_at": "2024-03-14T12:00:00.000Z"
    },
    {
      "batch_id": "batch-2",
      "status": "processing",
      "ids": [4, 5, 6],
      "created_at": "2024-03-14T12:00:05.000Z"
    }
  ],
  "created_at": "2024-03-14T12:00:00.000Z",
  "updated_at": "2024-03-14T12:00:05.000Z"
}
```

### 3. Health Check
```http
GET /health
```

Response:
```json
{
  "status": "ok",
  "timestamp": "2024-03-14T12:00:00.000Z"
}
```

## Rate Limiting

The system implements strict rate limiting:
- Maximum 1 batch processed every 5 seconds
- Processing delays based on priority:
  - High priority: 1 second processing time
  - Medium priority: 2 seconds processing time
  - Low priority: 3 seconds processing time

## Priority Processing

Batches are processed in the following order:
1. Priority level (HIGH > MEDIUM > LOW)
2. Creation time (FIFO within same priority)

## System Statistics

The system provides real-time statistics including:
- Total number of ingestions
- Total number of batches
- Status counts (pending, processing, completed)
- Current queue length
- Processing status

## Getting Started

1. Install dependencies:
```bash
npm install
```

2. Start the server:
```bash
npm start
```

The server will start on port 3000 by default. You can change this by setting the `PORT` environment variable.

## Environment Variables

- `PORT`: Server port (default: 3000)

## Error Handling

The system includes comprehensive error handling:
- Input validation
- Rate limit enforcement
- Priority validation
- Status validation
- Database state validation

## Testing

You can test the API using curl or any HTTP client:

```bash
# Create an ingestion
curl -X POST http://localhost:3000/ingestion \
  -H "Content-Type: application/json" \
  -d '{"ids": [1,2,3,4,5], "priority": "high"}'

# Check ingestion status
curl http://localhost:3000/ingestion/{ingestion_id}

# Check system health
curl http://localhost:3000/health
```

## License

MIT 