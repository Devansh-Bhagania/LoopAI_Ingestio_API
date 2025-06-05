# Data Ingestion API System

A RESTful API system for handling data ingestion requests with priority-based processing and rate limiting.

## Features

- Asynchronous batch processing
- Priority-based queue management
- Rate limiting (3 IDs per 5 seconds)
- Status tracking for ingestion requests
- Supabase PostgreSQL integration

## Prerequisites

- Node.js (v14 or higher)
- npm or yarn
- Supabase account and project

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd data-ingestion-api
```

2. Install dependencies:
```bash
npm install
```

3. Create a `.env` file in the root directory with the following variables:
```
PORT=5000
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

4. Create the following tables in your Supabase database:

```sql
-- Create ingestions table
CREATE TABLE ingestions (
  ingestion_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create batches table
CREATE TABLE batches (
  batch_id TEXT PRIMARY KEY,
  ingestion_id TEXT REFERENCES ingestions(ingestion_id),
  ids INTEGER[] NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

5. Build the project:
```bash
npm run build
```

## Running the Application

Development mode:
```bash
npm run dev
```

Production mode:
```bash
npm start
```

## API Endpoints

### POST /ingest
Creates a new ingestion request.

Request body:
```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

Response:
```json
{
  "ingestion_id": "uuid-string"
}
```

### GET /status/:ingestionId
Retrieves the status of an ingestion request.

Response:
```json
{
  "ingestion_id": "uuid-string",
  "status": "triggered",
  "batches": [
    {
      "batch_id": "uuid-string",
      "ids": [1, 2, 3],
      "status": "completed"
    }
  ]
}
```

## Testing

Run the test suite:
```bash
npm test
```

## Implementation Details

- The system processes IDs in batches of 3
- Rate limiting is enforced (1 batch per 5 seconds)
- Priority levels: HIGH, MEDIUM, LOW
- Processing order is determined by priority and creation time
- Status tracking for each batch and overall ingestion
- Asynchronous processing using a queue system

## Error Handling

The API includes validation for:
- Request format
- ID range (1 to 10^9+7)
- Priority values
- Non-existent ingestion IDs

## License

MIT 