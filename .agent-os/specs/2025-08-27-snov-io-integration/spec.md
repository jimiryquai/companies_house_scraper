# Spec Requirements Document

> Spec: snov-io-integration
> Created: 2025-08-27
> Status: Planning

## Overview

Integrate Snov.io API services to enrich Companies House officer data with contact information (email addresses) and company domains. This integration will implement a domain-first strategy where we discover company domains, then find officer email addresses using Snov.io's comprehensive email finding capabilities.

The integration leverages our existing queue system to efficiently batch requests, manage API rate limits, and implement credit tracking for cost management.

## User Stories

**As a data analyst**, I want to access officer email addresses so that I can conduct outreach and research on company personnel.

**As a business developer**, I want to find company domains automatically so that I can research company websites and digital presence.

**As a system administrator**, I want credit usage tracking so that I can monitor and control API costs.

**As a data consumer**, I want cached email and domain data so that I don't repeatedly consume API credits for the same information.

## Spec Scope

### Core Features
- **Domain Discovery**: Find company domains from company names using Snov.io Domain Search API
- **Email Finding**: Discover officer email addresses using domains and officer names
- **Queue Integration**: Use existing queue system with HIGH priority for domain discovery requests
- **Credit Management**: Track and monitor API credit usage with alerts
- **Data Caching**: Store discovered domains and emails in database to avoid redundant API calls
- **Webhook Handling**: Process Snov.io webhooks for bulk operation status updates

### Technical Integration
- Database schema extensions for domains and emails storage
- API service layer for Snov.io integration
- Queue job definitions for domain and email discovery
- Webhook endpoint for processing bulk operation callbacks
- Credit tracking and monitoring system

## Out of Scope

- Email validation or verification beyond Snov.io's built-in capabilities
- Integration with other email finding services
- Bulk email sending or marketing automation features
- Real-time email discovery (will use queue-based approach)
- Integration with email marketing platforms

## Expected Deliverable

A complete Snov.io integration system that:

1. **Discovers company domains** from Companies House company names with 80%+ success rate
2. **Finds officer email addresses** using discovered domains and officer names
3. **Manages API credits** with tracking, alerting, and budget controls
4. **Caches all data** to minimize API usage and costs
5. **Processes requests efficiently** through the existing queue system
6. **Handles bulk operations** via webhook callbacks
7. **Provides monitoring** for success rates, credit usage, and system health

Success criteria:
- Domain discovery for 80% of active companies
- Email discovery for 60% of officers with domains
- Credit usage tracking with 99% accuracy
- Queue processing with <5 minute average completion time
- Database performance maintains current standards

## Spec Documentation

- Tasks: @.agent-os/specs/2025-08-27-snov-io-integration/tasks.md
- Technical Specification: @.agent-os/specs/2025-08-27-snov-io-integration/sub-specs/technical-spec.md
- Database Schema: @.agent-os/specs/2025-08-27-snov-io-integration/sub-specs/database-schema.md
- API Specification: @.agent-os/specs/2025-08-27-snov-io-integration/sub-specs/api-spec.md
