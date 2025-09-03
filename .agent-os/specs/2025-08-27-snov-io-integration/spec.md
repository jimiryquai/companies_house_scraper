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

**As a system administrator**, I want credit usage tracking so that I can monitor monthly consumption without restrictive daily limits.

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

A complete Snov.io integration system that implements a 10-step credit-aware workflow:

1. **Monitors CH Streaming API** for company change events
2. **Calls CH REST API** for every company to get `company_status_detail`
3. **Filters strike-off companies** (`company_status_detail == "active-proposal-to-strike-off"`)
4. **Checks credits before domain search** using `GET /v1/get-balance`
5. **Searches company domains** (only if credits > 0) using Snov.io v2 API with polling
6. **Records actual credit consumption** via post-operation balance check
7. **Fetches company officers** (if domain found) via CH REST API
8. **Checks credits before email search** using `GET /v1/get-balance`
9. **Finds officer emails** (only if credits > 0) using Snov.io v2 API with polling
10. **Records final credit consumption** and updates balance tracking

**Key Features**:
- **Credit-Aware Operations**: Never assumes credit consumption, always checks before/after
- **Non-Blocking CH API**: Companies House REST API continues regardless of Snov.io credit status  
- **Simple Exhaustion Policy**: Stop Snov.io calls when credits = 0, no daily rationing
- **Real Credit Consumption Tracking**: Operation-based tracking with actual balance verification

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
