# Database Schema Documentation

This directory contains the Entity Relationship Diagram (ERD) for the Companies House Scraper database schema.

## Files

- **`database_schema.d2`** - D2 source file containing the complete database schema definition
- **`database_schema.svg`** - Generated SVG diagram showing all tables, columns, and relationships

## Schema Overview

The database contains **13 tables** organized into the following categories:

### 🏢 Core Tables
- **`companies`** - Primary company data from Companies House
- **`officers`** - Company officers and their details

### 🌐 Snov.io Integration
- **`company_domains`** - Domains discovered for companies via Snov.io
- **`officer_emails`** - Email addresses found for officers
- **`snov_credit_usage`** - API credit consumption tracking
- **`snov_webhooks`** - Webhook events from Snov.io API

### ⚙️ Processing & Queue Management
- **`queue_jobs`** - Job queue for API processing
- **`company_processing_state`** - Company processing status tracking
- **`enrichment_state`** - Email enrichment workflow state

### 📡 Streaming & Monitoring
- **`stream_events`** - Real-time Companies House events
- **`api_rate_limit_log`** - API rate limiting and monitoring

### 🔧 System Tables
- **`metadata`** - System configuration and state
- **`schema_version`** - Database migration versioning

## Key Relationships

- **Companies ↔ Officers**: One-to-many relationship
- **Companies ↔ Domains**: One-to-many via Snov.io discovery
- **Officers ↔ Emails**: One-to-many via Snov.io email finder
- **Processing States**: Multiple tracking tables linked to companies

## Database Schema Version

Current schema version: **6**  
Applied migrations: 001 through 006  
All Snov.io integration tables and streaming functionality included.

## Viewing the ERD

Open `database_schema.svg` in any web browser or SVG viewer to see the complete visual representation of the database schema with color-coded table categories and relationship lines.

## Regenerating the Diagram

To update the ERD after schema changes:

```bash
# Install d2 (if not already installed)
curl -fsSL https://d2lang.com/install.sh | sh

# Generate new SVG
export PATH="$HOME/.local/bin:$PATH"
d2 docs/database_schema.d2 docs/database_schema.svg
```