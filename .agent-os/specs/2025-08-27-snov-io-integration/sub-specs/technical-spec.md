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

### Domain-First Strategy
1. **Company Domain Discovery**: Use Snov.io Domain Search API with company name
2. **Domain Validation**: Verify and score domain confidence before storage
3. **Email Discovery**: Use verified domains with officer names for email finding
4. **Data Enrichment**: Link discovered emails back to officer records

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
- **Budget Controls**: Configurable credit limits with auto-pause functionality
- **Cost Optimization**: Smart caching and batch operations to minimize API calls
- **Alerting**: Email/webhook alerts at 80% and 95% credit usage thresholds

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
