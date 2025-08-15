# Database Schema

This is the database schema implementation for the spec detailed in @.agent-os/specs/2025-08-15-streaming-api-integration/spec.md

> Created: 2025-08-15
> Version: 1.0.0

## Schema Changes

### Extend Companies Table
Add streaming-related metadata columns to the existing companies table:

```sql
-- Add columns to existing companies table
ALTER TABLE companies ADD COLUMN stream_last_updated TEXT;
ALTER TABLE companies ADD COLUMN stream_event_id TEXT;
ALTER TABLE companies ADD COLUMN stream_event_type TEXT;
ALTER TABLE companies ADD COLUMN stream_processing_status TEXT DEFAULT 'pending';
```

### New Stream Metadata Table
Create dedicated table for tracking stream operations:

```sql
CREATE TABLE IF NOT EXISTS stream_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_type TEXT NOT NULL,  -- 'company-profile'
    last_event_id TEXT,
    last_processed_timestamp TEXT,
    connection_status TEXT DEFAULT 'disconnected',
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

### Stream Events Log Table
Track all processed events for debugging and monitoring:

```sql
CREATE TABLE IF NOT EXISTS stream_events_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    company_number TEXT,
    event_data TEXT,  -- JSON blob of full event
    processing_status TEXT DEFAULT 'pending',  -- pending, processed, failed
    error_message TEXT,
    received_at TEXT NOT NULL,
    processed_at TEXT,
    INDEX idx_company_number (company_number),
    INDEX idx_event_type (event_type),
    INDEX idx_processing_status (processing_status),
    INDEX idx_received_at (received_at)
);
```

## Migrations

### Migration 001: Add Streaming Columns
```sql
-- Migration: 001_add_streaming_columns.sql
BEGIN TRANSACTION;

-- Add streaming metadata columns to companies table
ALTER TABLE companies ADD COLUMN stream_last_updated TEXT;
ALTER TABLE companies ADD COLUMN stream_event_id TEXT;
ALTER TABLE companies ADD COLUMN stream_event_type TEXT;
ALTER TABLE companies ADD COLUMN stream_processing_status TEXT DEFAULT 'pending';

-- Create index for faster streaming queries
CREATE INDEX IF NOT EXISTS idx_companies_stream_status 
ON companies(stream_processing_status);

CREATE INDEX IF NOT EXISTS idx_companies_stream_updated 
ON companies(stream_last_updated);

COMMIT;
```

### Migration 002: Create Stream Tables
```sql
-- Migration: 002_create_stream_tables.sql
BEGIN TRANSACTION;

-- Stream metadata table
CREATE TABLE IF NOT EXISTS stream_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_type TEXT NOT NULL,
    last_event_id TEXT,
    last_processed_timestamp TEXT,
    connection_status TEXT DEFAULT 'disconnected',
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Stream events log table
CREATE TABLE IF NOT EXISTS stream_events_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    company_number TEXT,
    event_data TEXT,
    processing_status TEXT DEFAULT 'pending',
    error_message TEXT,
    received_at TEXT NOT NULL,
    processed_at TEXT
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_stream_events_company 
ON stream_events_log(company_number);

CREATE INDEX IF NOT EXISTS idx_stream_events_type 
ON stream_events_log(event_type);

CREATE INDEX IF NOT EXISTS idx_stream_events_status 
ON stream_events_log(processing_status);

CREATE INDEX IF NOT EXISTS idx_stream_events_received 
ON stream_events_log(received_at);

-- Insert initial metadata record
INSERT INTO stream_metadata (
    stream_type, 
    created_at, 
    updated_at
) VALUES (
    'company-profile',
    datetime('now'),
    datetime('now')
);

COMMIT;
```

### Data Integrity Constraints
- `event_id` must be unique across all stream events
- `company_number` should reference existing companies where applicable
- Timestamps must be in ISO 8601 format
- `processing_status` must be one of: 'pending', 'processed', 'failed'
- `connection_status` must be one of: 'connected', 'disconnected', 'error'