# Snov.io Integration - Lite Summary

Implement a 10-step credit-aware workflow to enrich Companies House officer data with email addresses and company domains using Snov.io API, ensuring credit checks before every operation and non-blocking CH API processing.

## Credit-Aware Workflow (10 Steps)
1. CH Streaming API → company change events
2. CH REST API → get `company_status_detail` (MANDATORY for all companies)  
3. Filter → only `"active-proposal-to-strike-off"` companies proceed
4. Credit Check → `GET /v1/get-balance` before domain search
5. Domain Search → Snov.io v2 API (only if credits > 0)
6. Credit Update → record actual consumption post-domain search  
7. Officer Fetch → CH REST API (if domain found)
8. Credit Check → `GET /v1/get-balance` before email search
9. Email Search → Snov.io v2 API (only if credits > 0)
10. Final Credit Update → record final consumption

## Key Requirements
- **Credit-Aware**: Never assume consumption, always check before/after operations
- **Non-Blocking**: CH REST API continues regardless of Snov.io credit status
- **Simple Exhaustion**: Stop Snov.io when credits = 0, no daily rationing
- **Real Tracking**: Operation-based tracking with balance verification
