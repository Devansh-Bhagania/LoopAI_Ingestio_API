# Data Ingestion API System

A RESTful API system built with Express.js and TypeScript for handling data ingestion requests and status tracking. The system processes data in batches asynchronously, respects rate limits, and prioritizes requests based on their priority level.

## Features

- **Ingestion API (`POST /ingest`)**: Submit data ingestion requests with a list of IDs and priority level (HIGH, MEDIUM, LOW).
- **Status API (`GET /status/:ingestionId`)**: Check the processing status of an ingestion request, including details of each batch.
- **Asynchronous Processing**: Batches are processed asynchronously, with a rate limit of 1 batch per 5 seconds (max 3 IDs per 5 seconds).
- **Priority-Based Processing**: Requests are processed based on priority (HIGH > MEDIUM > LOW) and creation time.
- **Simulated External API**: The system simulates fetching data from an external API with a delay.

## API Endpoints

### 1. Ingestion API

**Endpoint:** `POST /ingest`

**Input:**
```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

**Output:**
```json
{
  "ingestion_id": "abc123"
}
```

### 2. Status API

**Endpoint:** `GET /status/:ingestionId`

**Input:** The ingestion_id returned by the ingestion API.

**Output:**
```json
{
  "ingestion_id": "abc123",
  "status": "triggered",
  "batches": [
    {
      "batch_id": "batch-1",
      "ids": [1, 2, 3],
      "status": "completed",
      "created_at": "2024-06-05T12:34:56.789Z"
    },
    {
      "batch_id": "batch-2",
      "ids": [4, 5],
      "status": "triggered",
      "created_at": "2024-06-05T12:34:56.789Z"
    }
  ]
}
```

## Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd data-ingestion-api
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Build the project:**
   ```bash
   npm run build
   ```

4. **Run the server:**
   ```bash
   npm start
   ```

5. **Run tests:**
   ```bash
   npm test
   ```

## Testing

The project includes extensive tests to verify the core functionality, including:
- Request validation
- Priority-based processing
- Rate limiting
- Status tracking

Run the tests with:
```bash
npm test
```

## Screenshot of Test Run

![Test Run Screenshot](test-run-screenshot.png)

## Notes

- This is a simulated environment—no real external API calls are made.
- Focus on core functionality (ingestion and status tracking). Optional features like error recovery or retry logic can be added if time permits.

## License

This project is licensed under the MIT License. 