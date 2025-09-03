# Database Schema

This is the database schema implementation for the spec detailed in @.agent-os/specs/2025-08-27-snov-io-integration/spec.md

> Created: 2025-08-27
> Version: 1.0.0

## Schema Changes

### New Tables

#### company_domains
```sql
CREATE TABLE company_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    confidence_score REAL,
    discovery_method TEXT, -- 'snov_domain_search', 'manual', 'inferred'
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_verified TIMESTAMP,
    is_primary BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'active', -- 'active', 'inactive', 'unverified'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (company_id) REFERENCES companies(company_number),
    UNIQUE(company_id, domain)
);

CREATE INDEX idx_company_domains_company_id ON company_domains(company_id);
CREATE INDEX idx_company_domains_domain ON company_domains(domain);
CREATE INDEX idx_company_domains_primary ON company_domains(company_id, is_primary);
```

#### officer_emails
```sql
CREATE TABLE officer_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    officer_id TEXT NOT NULL,
    email TEXT NOT NULL,
    email_type TEXT, -- 'work', 'personal', 'generic', 'catch_all'
    verification_status TEXT, -- 'valid', 'invalid', 'catch_all', 'unknown', 'disposable'
    confidence_score REAL,
    domain TEXT,
    discovery_method TEXT, -- 'snov_email_finder', 'snov_bulk_search', 'manual'
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_verified TIMESTAMP,
    snov_request_id TEXT, -- For tracking bulk operations
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (officer_id) REFERENCES officers(id),
    UNIQUE(officer_id, email)
);

CREATE INDEX idx_officer_emails_officer_id ON officer_emails(officer_id);
CREATE INDEX idx_officer_emails_email ON officer_emails(email);
CREATE INDEX idx_officer_emails_domain ON officer_emails(domain);
CREATE INDEX idx_officer_emails_status ON officer_emails(verification_status);
```

#### snov_credit_usage
```sql
CREATE TABLE snov_credit_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_type TEXT NOT NULL, -- 'domain_search', 'email_finder', 'email_verifier', 'bulk_email_finder'
    credits_consumed INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    request_id TEXT, -- Snov.io request ID for tracking
    company_id TEXT,
    officer_id TEXT,
    response_data TEXT, -- JSON response for audit
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (company_id) REFERENCES companies(company_number),
    FOREIGN KEY (officer_id) REFERENCES officers(id)
);

CREATE INDEX idx_snov_credit_usage_timestamp ON snov_credit_usage(timestamp);
CREATE INDEX idx_snov_credit_usage_operation ON snov_credit_usage(operation_type);
CREATE INDEX idx_snov_credit_usage_company ON snov_credit_usage(company_id);
```

#### snov_webhooks
```sql
CREATE TABLE snov_webhooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    webhook_id TEXT UNIQUE NOT NULL,
    event_type TEXT NOT NULL, -- 'bulk_email_finder_completed', 'bulk_domain_search_completed'
    status TEXT NOT NULL, -- 'received', 'processed', 'error'
    request_id TEXT, -- Original Snov.io request ID
    payload TEXT NOT NULL, -- Full webhook payload JSON
    processed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(webhook_id)
);

CREATE INDEX idx_snov_webhooks_status ON snov_webhooks(status);
CREATE INDEX idx_snov_webhooks_event_type ON snov_webhooks(event_type);
CREATE INDEX idx_snov_webhooks_request_id ON snov_webhooks(request_id);
```

### Table Modifications

#### queue_jobs (extend existing)
```sql
-- Add new job types for Snov.io operations
-- Existing table, new job types:
-- 'snov_domain_discovery'
-- 'snov_email_discovery'
-- 'snov_bulk_email_discovery'
-- 'snov_webhook_processing'

-- Add new columns for Snov.io specific data
ALTER TABLE queue_jobs ADD COLUMN snov_request_id TEXT;
ALTER TABLE queue_jobs ADD COLUMN snov_credits_estimated INTEGER;

CREATE INDEX idx_queue_jobs_snov_request ON queue_jobs(snov_request_id);
```

## Migrations

### Migration 1: Create Snov.io Tables
```sql
-- Migration: 2025-08-27-001-create-snov-tables.sql
-- Create all Snov.io related tables

BEGIN TRANSACTION;

-- Create company_domains table
CREATE TABLE company_domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    confidence_score REAL,
    discovery_method TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_verified TIMESTAMP,
    is_primary BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (company_id) REFERENCES companies(company_number),
    UNIQUE(company_id, domain)
);

CREATE INDEX idx_company_domains_company_id ON company_domains(company_id);
CREATE INDEX idx_company_domains_domain ON company_domains(domain);
CREATE INDEX idx_company_domains_primary ON company_domains(company_id, is_primary);

-- Create officer_emails table
CREATE TABLE officer_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    officer_id TEXT NOT NULL,
    email TEXT NOT NULL,
    email_type TEXT,
    verification_status TEXT,
    confidence_score REAL,
    domain TEXT,
    discovery_method TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_verified TIMESTAMP,
    snov_request_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (officer_id) REFERENCES officers(id),
    UNIQUE(officer_id, email)
);

CREATE INDEX idx_officer_emails_officer_id ON officer_emails(officer_id);
CREATE INDEX idx_officer_emails_email ON officer_emails(email);
CREATE INDEX idx_officer_emails_domain ON officer_emails(domain);
CREATE INDEX idx_officer_emails_status ON officer_emails(verification_status);

-- Create snov_credit_usage table
CREATE TABLE snov_credit_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_type TEXT NOT NULL,
    credits_consumed INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    request_id TEXT,
    company_id TEXT,
    officer_id TEXT,
    response_data TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (company_id) REFERENCES companies(company_number),
    FOREIGN KEY (officer_id) REFERENCES officers(id)
);

CREATE INDEX idx_snov_credit_usage_timestamp ON snov_credit_usage(timestamp);
CREATE INDEX idx_snov_credit_usage_operation ON snov_credit_usage(operation_type);
CREATE INDEX idx_snov_credit_usage_company ON snov_credit_usage(company_id);

-- Create snov_webhooks table
CREATE TABLE snov_webhooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    webhook_id TEXT UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    request_id TEXT,
    payload TEXT NOT NULL,
    processed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(webhook_id)
);

CREATE INDEX idx_snov_webhooks_status ON snov_webhooks(status);
CREATE INDEX idx_snov_webhooks_event_type ON snov_webhooks(event_type);
CREATE INDEX idx_snov_webhooks_request_id ON snov_webhooks(request_id);

COMMIT;
```

### Migration 2: Extend Queue Jobs
```sql
-- Migration: 2025-08-27-002-extend-queue-jobs.sql
-- Add Snov.io specific columns to queue_jobs

BEGIN TRANSACTION;

ALTER TABLE queue_jobs ADD COLUMN snov_request_id TEXT;
ALTER TABLE queue_jobs ADD COLUMN snov_credits_estimated INTEGER;

CREATE INDEX idx_queue_jobs_snov_request ON queue_jobs(snov_request_id);

COMMIT;
```
