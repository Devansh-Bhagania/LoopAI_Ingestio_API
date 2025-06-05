export enum Priority {
  HIGH = 'HIGH',
  MEDIUM = 'MEDIUM',
  LOW = 'LOW'
}

export enum BatchStatus {
  YET_TO_START = 'yet_to_start',
  TRIGGERED = 'triggered',
  COMPLETED = 'completed'
}

export interface IngestionRequest {
  ids: number[];
  priority: Priority;
}

export interface Batch {
  batch_id: string;
  ingestion_id: string;
  ids: number[];
  status: BatchStatus;
  created_at: Date;
}

export interface IngestionStatus {
  ingestion_id: string;
  status: BatchStatus;
  batches: Batch[];
}

export interface IngestionResponse {
  ingestion_id: string;
} 