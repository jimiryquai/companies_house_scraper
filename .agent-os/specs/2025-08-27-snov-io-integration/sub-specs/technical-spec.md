# Technical Specification

This is the technical specification for the spec detailed in @.agent-os/specs/2025-08-27-snov-io-integration/spec.md

> Created: 2025-08-27
> Version: 1.0.0

## Technical Requirements

### API Integration Requirements

- **Snov.io API Client**: RESTful client with authentication, rate limiting, and error handling
- **Domain Search API**: Find company domains from company names with confidence scoring
- **Email Finder API**: Discover officer emails using domains and names with verification status
- **Credit Tracking**: Real-time credit usage monitoring with threshold alerts
- **Webhook Processing**: Handle bulk operation callbacks with proper validation and security

### Queue System Integration

- **Domain Discovery Jobs**: HIGH priority queue jobs for domain finding operations
- **Email Discovery Jobs**: NORMAL priority queue jobs for email finding operations
- **Batch Processing**: Support for bulk operations with progress tracking
- **Error Handling**: Retry logic with exponential backoff for API failures
- **Job Prioritization**: Smart scheduling based on data freshness and user requests

### Performance Requirements

- **Response Time**: Domain discovery jobs complete within 2 minutes average
- **Throughput**: Process 1000+ domain lookups per hour within API limits
- **Cache Hit Rate**: 90%+ cache hit rate for repeated domain/email lookups
- **Database Performance**: Email/domain queries execute in <100ms
- **API Rate Limiting**: Respect Snov.io rate limits with smart request spacing

## Approach

### Credit-Aware E2E Workflow

1. **CH Streaming Event**: Receive company change notification from Companies House Streaming API
2. **CH REST API Call**: Get company details via `GET /company/{companyNumber}` to check `company_status_detail`
3. **Strike-off Filter**: Only proceed if `company_status_detail == "active-proposal-to-strike-off"`
4. **Pre-Domain Credit Check**: `GET /v1/get-balance` to verify credits available before domain search
5. **Domain Search** (if credits > 0): Snov.io v2 API `POST /company-domain-by-name/start` + polling
6. **Post-Domain Credit Check**: `GET /v1/get-balance` to record actual credit consumption and update balance
7. **Officer Fetch** (if domain found): CH REST API `GET /company/{companyNumber}/officers`
8. **Pre-Email Credit Check**: `GET /v1/get-balance` to verify credits before email search
9. **Email Search** (if credits > 0): Snov.io v2 API `POST /emails-by-domain-by-name/start` + polling
10. **Final Credit Check**: `GET /v1/get-balance` to record final consumption and update balance

**Critical Requirements**:

- CH REST API calls MUST continue regardless of Snov.io credit status
- Credit checks MUST happen before each Snov.io operation (cannot assume consumption)
- Operations may consume 0 credits if no results found
- Simple exhaustion policy: stop Snov.io calls when credits = 0, no daily rationing

### Queue-Based Processing

```python
# Domain discovery flow
queue_job = {
    "type": "domain_discovery",
    "priority": "HIGH",
    "company_id": "12345",
    "company_name": "Example Ltd",
    "retry_count": 0
}

# Email discovery flow
queue_job = {
    "type": "email_discovery",
    "priority": "NORMAL",
    "officer_id": "67890",
    "officer_name": "John Smith",
    "domain": "example.com",
    "retry_count": 0
}
```

### Caching Strategy

- **Domain Cache**: Store domains with confidence scores and last_verified timestamp
- **Email Cache**: Store emails with verification status and discovery date
- **TTL Policy**: 30 days for domains, 90 days for verified emails
- **Cache Invalidation**: Manual refresh capability for stale data

### Credit Management

- **Usage Tracking**: Real-time credit consumption with operation type breakdown
- **Simple Exhaustion Policy**: Stop Snov.io API calls when credits reach zero - no daily rationing
- **Cost Optimization**: Smart caching and batch operations to minimize API calls
- **CH API Independence**: Companies House REST API continues regardless of Snov.io credit status
- **Monthly Credit Pool**: 5000+ credits per month (scalable to 20k/50k/100k as needed)
- **Monitoring**: Credit balance monitoring without restrictive thresholds

## External Dependencies

### Snov.io API Services

- **Domain Search API**: Find domains from company names
- **Email Finder API**: Discover emails using domain + person name
- **Email Verifier API**: Verify email address deliverability
- **Account API**: Monitor credit balance and usage statistics

### Python Packages

- **httpx**: Async HTTP client for API requests
- **pydantic**: Data validation and serialization
- **tenacity**: Retry logic with exponential backoff
- **cryptography**: Webhook signature verification
- **python-dotenv**: Environment variable management

### Infrastructure

- **Redis Cache**: Optional caching layer for high-performance lookups
- **Webhook Endpoint**: HTTPS endpoint for receiving Snov.io callbacks
- **Monitoring**: Integration with existing health monitoring system
- **Logging**: Structured logging for audit trails and debugging
