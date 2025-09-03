# API Specification

This is the API specification for the spec detailed in @.agent-os/specs/2025-08-27-snov-io-integration/spec.md

> Created: 2025-08-27
> Version: 1.0.0

## Endpoints

### Webhook Endpoints

#### POST /webhooks/snov-io
**Purpose**: Receive webhook notifications from Snov.io for bulk operations

**Headers**:
- `Content-Type: application/json`
- `X-Snov-Signature: <signature>` (for verification)

**Request Body**:
```json
{
    "event_type": "bulk_email_finder_completed",
    "request_id": "snov_req_12345",
    "status": "completed",
    "credits_consumed": 25,
    "results_count": 100,
    "webhook_id": "webhook_67890",
    "timestamp": "2025-08-27T10:30:00Z",
    "data": {
        "download_url": "https://snov.io/download/results_12345.csv",
        "expires_at": "2025-08-28T10:30:00Z"
    }
}
```

**Response**:
```json
{
    "status": "accepted",
    "webhook_id": "webhook_67890",
    "processed_at": "2025-08-27T10:30:15Z"
}
```

**Status Codes**:
- `200 OK`: Webhook received and queued for processing
- `400 Bad Request`: Invalid webhook payload or signature
- `401 Unauthorized`: Invalid webhook signature
- `500 Internal Server Error`: Processing error

### Internal API Endpoints

#### GET /api/companies/{company_id}/domains
**Purpose**: Retrieve discovered domains for a company

**Response**:
```json
{
    "company_id": "12345678",
    "domains": [
        {
            "domain": "example.com",
            "confidence_score": 0.95,
            "is_primary": true,
            "discovery_method": "snov_domain_search",
            "discovered_at": "2025-08-27T09:15:00Z",
            "status": "active"
        }
    ]
}
```

#### GET /api/officers/{officer_id}/emails
**Purpose**: Retrieve discovered emails for an officer

**Response**:
```json
{
    "officer_id": "officer_789",
    "emails": [
        {
            "email": "john.smith@example.com",
            "email_type": "work",
            "verification_status": "valid",
            "confidence_score": 0.88,
            "domain": "example.com",
            "discovery_method": "snov_email_finder",
            "discovered_at": "2025-08-27T10:00:00Z"
        }
    ]
}
```

#### POST /api/discovery/domain
**Purpose**: Queue domain discovery job for a company

**Request Body**:
```json
{
    "company_id": "12345678",
    "company_name": "Example Ltd",
    "priority": "HIGH"
}
```

**Response**:
```json
{
    "job_id": "job_abc123",
    "status": "queued",
    "estimated_completion": "2025-08-27T10:35:00Z"
}
```

#### POST /api/discovery/email
**Purpose**: Queue email discovery job for an officer

**Request Body**:
```json
{
    "officer_id": "officer_789",
    "officer_name": "John Smith",
    "domain": "example.com",
    "priority": "NORMAL"
}
```

#### GET /api/snov/credits
**Purpose**: Get current Snov.io credit balance and usage statistics

**Response**:
```json
{
    "current_balance": 8750,
    "monthly_usage": 1250,
    "usage_by_operation": {
        "domain_search": 300,
        "email_finder": 800,
        "email_verifier": 150
    },
    "status": "available",
    "exhausted": false
}
```

## Controllers

### WebhookController
**File**: `src/api/controllers/webhook_controller.py`

**Responsibilities**:
- Validate webhook signatures from Snov.io
- Parse and validate webhook payloads
- Queue webhook processing jobs
- Return appropriate HTTP responses

**Methods**:
```python
class WebhookController:
    async def handle_snov_webhook(self, request: Request) -> JSONResponse
    def verify_webhook_signature(self, payload: str, signature: str) -> bool
    def parse_webhook_payload(self, payload: dict) -> WebhookData
```

### DiscoveryController
**File**: `src/api/controllers/discovery_controller.py`

**Responsibilities**:
- Handle domain and email discovery requests
- Queue discovery jobs with appropriate priorities
- Return job status and results
- Validate request parameters

**Methods**:
```python
class DiscoveryController:
    async def queue_domain_discovery(self, request: DomainDiscoveryRequest) -> JobResponse
    async def queue_email_discovery(self, request: EmailDiscoveryRequest) -> JobResponse
    async def get_company_domains(self, company_id: str) -> DomainsResponse
    async def get_officer_emails(self, officer_id: str) -> EmailsResponse
```

### CreditsController
**File**: `src/api/controllers/credits_controller.py`

**Responsibilities**:
- Monitor Snov.io credit usage
- Provide usage statistics and analytics  
- Return current balance and exhaustion status
- Track monthly credit consumption without restrictive limits

**Methods**:
```python
class CreditsController:
    async def get_credit_status(self) -> CreditsResponse
    async def get_usage_statistics(self, period: str) -> UsageResponse
    def check_credit_thresholds(self) -> List[Alert]
```

### Data Models

#### WebhookData
```python
@dataclass
class WebhookData:
    event_type: str
    request_id: str
    status: str
    credits_consumed: int
    results_count: int
    webhook_id: str
    timestamp: datetime
    data: dict
```

#### DomainDiscoveryRequest
```python
@dataclass
class DomainDiscoveryRequest:
    company_id: str
    company_name: str
    priority: str = "NORMAL"
```

#### EmailDiscoveryRequest
```python
@dataclass
class EmailDiscoveryRequest:
    officer_id: str
    officer_name: str
    domain: str
    priority: str = "NORMAL"
```

### Error Handling

**Custom Exceptions**:
- `InvalidWebhookSignatureError`: Invalid webhook signature
- `SnovApiError`: Snov.io API related errors
- `CreditsExhaustedError`: No credits remaining for Snov.io operations
- `InvalidDiscoveryRequestError`: Malformed discovery requests

**Error Response Format**:
```json
{
    "error": {
        "code": "INVALID_WEBHOOK_SIGNATURE",
        "message": "Webhook signature verification failed",
        "timestamp": "2025-08-27T10:30:00Z"
    }
}
```
