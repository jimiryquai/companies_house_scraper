# Company Processing State Manager - Database Schema

This schema supports bulletproof rate limiting and state management for autonomous cloud deployment.

## Schema Changes

### New Table: company_processing_state

```sql
CREATE TABLE company_processing_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_number TEXT NOT NULL,
    processing_state TEXT NOT NULL DEFAULT 'detected',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status_queued_at TIMESTAMP NULL,
    status_fetched_at TIMESTAMP NULL,
    officers_queued_at TIMESTAMP NULL,
    officers_fetched_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    status_retry_count INTEGER DEFAULT 0,
    officers_retry_count INTEGER DEFAULT 0,
    status_request_id TEXT NULL,
    officers_request_id TEXT NULL,
    last_error TEXT NULL,
    last_error_at TIMESTAMP NULL,
    last_429_response_at TIMESTAMP NULL,
    rate_limit_violations INTEGER DEFAULT 0,
    FOREIGN KEY (company_number) REFERENCES companies(company_number),
    UNIQUE(company_number)
);
```

### Index Creation

```sql
CREATE INDEX idx_company_processing_state_status ON company_processing_state(processing_state);
CREATE INDEX idx_company_processing_state_updated ON company_processing_state(updated_at);
CREATE INDEX idx_company_processing_state_company ON company_processing_state(company_number);
CREATE INDEX idx_company_processing_state_requests ON company_processing_state(status_request_id, officers_request_id);
CREATE INDEX idx_company_processing_state_violations ON company_processing_state(rate_limit_violations, last_429_response_at);
```

### Rate Limit Tracking Table

```sql
CREATE TABLE api_rate_limit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    request_type TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    company_number TEXT NULL,
    request_id TEXT NULL,
    processing_time_ms INTEGER NULL
);

CREATE INDEX idx_api_rate_limit_timestamp ON api_rate_limit_log(timestamp);
CREATE INDEX idx_api_rate_limit_status ON api_rate_limit_log(response_status);
```

## Processing State Enumeration

- **detected** - Company detected via streaming API (NO API calls made yet)
- **status_queued** - Company status check request queued in PriorityQueueManager
- **status_fetched** - Company status retrieved via REST API through queue
- **strike_off_confirmed** - company_status_detail == "active-proposal-to-strike-off" confirmed
- **officers_queued** - Officer fetch request queued in PriorityQueueManager
- **officers_fetched** - Officer data successfully retrieved and saved through queue
- **completed** - All processing steps finished successfully
- **failed** - Processing failed after maximum retry attempts (with rate limit compliance)

## Critical Requirements

- **ZERO direct API calls** - All requests must go through queue
- **600 calls/5min enforcement** - Strict rate limit compliance to prevent API bans
- **429 response handling** - Automatic queue throttling and emergency shutdown
- **State persistence** - Processing survives service restarts
- **Cloud deployment ready** - Autonomous operation without manual intervention
