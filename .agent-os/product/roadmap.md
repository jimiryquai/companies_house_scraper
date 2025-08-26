# Product Roadmap

## Phase 0: Already Completed (May 2025)

**Goal:** Initial lead generation system implementation
**Status:** ✅ Complete

### Completed Features

- [x] Bulk CSV data import - Process BasicCompanyDataAsOneFile CSV from Companies House `L`
- [x] Strike-off status filtering - Filter companies with "active-proposal-to-strike-off" status during import `M`
- [x] SQLite database implementation - Local storage for company and officer records `M`
- [x] Officer API integration - Fetch director information via Companies House API `L`
- [x] Rate limit handling - Respect 600 calls per 5 minutes API limit `M`
- [x] Resumable processing - Save progress for officer fetching to allow interruption/restart `M`
- [x] CSV export functionality - Export filtered data to CSV format for CRM import `S`
- [x] Batch processing - Memory-efficient handling of multi-GB CSV files `M`
- [x] YAML configuration - API keys and settings management `S`
- [x] Comprehensive logging - Detailed logging for debugging and monitoring `S`
- [x] Error handling - Robust retry logic for API failures `M`

## Phase 1: Data Foundation & Real-Time Monitoring (Current - August 2025)

**Goal:** Update data to current state while implementing real-time monitoring to prevent data gaps
**Success Criteria:** Successfully process August 15, 2025 bulk data snapshot, achieve continuous data coverage from August 15 onward, complete officer extraction for all distressed companies, achieve >95% data accuracy

### Track A: Bulk Data Processing (Largely Complete)

- [x] Process August 15, 2025 bulk data snapshot - Download and import latest BasicCompanyDataAsOneFile CSV `L`
- [x] Update officer data - Re-run officer fetching for new companies (ongoing: 60k/395k+ processed) `L`
- [ ] Database cleanup - Remove companies no longer in strike-off status (to be integrated with streaming) `M`
- [x] Performance optimization - Optimize officer fetching to reduce total processing time `M`

### Track B: Real-Time Monitoring (✅ COMPLETE - Production Ready)

- [x] Companies House Streaming API registration - Obtain streaming API key separate from REST API `M`
- [x] Streaming API client implementation - Build HTTP client for long-running connections `L`
- [x] Company status monitoring - Monitor company information stream for strike-off status changes `L`
- [x] Hybrid API approach - Combine streaming notifications with REST API detailed status `L`
- [x] Real-time officer fetching - Automatically fetch officers for new strike-off companies `L`
- [x] Main streaming service deployment - Production-ready streaming service with full integration `L`
- [x] Database cleanup automation - Real-time removal of companies leaving strike-off status `M`
- [x] End-to-end verification - Proven functionality with real Companies House data `M`

### Track C: Data Integration & Quality

- [ ] Stream-bulk data synchronization - Merge streaming updates with bulk processing results `M`
- [ ] Data quality validation - Verify imported data integrity and completeness across both sources `S`
- [ ] Duplicate detection - Handle companies appearing in both bulk and streaming data `M`
- [ ] Export enhancement - Add filtering options and custom field selection for CSV export `S`

### Dependencies

- Companies House API access and rate limits (REST API)
- Companies House Streaming API registration and access
- SQLite database optimization for large datasets
- Bulk data file availability from Companies House
- HTTP client library for long-running streaming connections

## Phase 2: Data Enrichment & CRM Integration

**Goal:** Enhance lead data quality and automate CRM delivery
**Success Criteria:** Apollo.io integration delivering enriched contact data, automated GoHighLevel synchronization operational, lead quality scores implemented

### Features

- [ ] Apollo.io data enrichment - Automatic contact information and company intelligence enhancement `L`
- [ ] GoHighLevel CRM integration - Direct export and synchronization with CRM system `L`
- [ ] Lead scoring system - Prioritize companies based on distress indicators and business potential `M`
- [ ] Automated processing pipeline - Scheduled data enrichment and CRM updates without manual intervention `M`
- [ ] Alert system - Notifications for high-priority leads and status changes `M`
- [ ] Contact verification - Validate email addresses and phone numbers before CRM delivery `M`

### Dependencies

- Apollo.io API credentials and integration approval
- GoHighLevel API access and configuration
- Real-time data pipeline from Phase 1
- Lead scoring algorithm development

## Phase 3: Cloud Deployment & Scale

**Goal:** Deploy to cloud infrastructure for reliable, scalable operation
**Success Criteria:** System running reliably in cloud, handling full UK company dataset during business hours, sub-second query response times

### Features

- [ ] Render deployment - Background Worker for streaming service with business hours scheduling `M`
- [ ] Database optimization - SQLite on persistent disk with proper indexing and WAL mode `S`
- [ ] Web interface development - User-friendly dashboard for lead management and monitoring `XL`
- [ ] Advanced filtering and search - Complex query capabilities with saved searches and alerts `M`
- [ ] API development - RESTful API for external integrations and mobile applications `L`
- [ ] Monitoring and analytics - Comprehensive system monitoring and lead generation analytics `M`
- [ ] Cost optimization - Automated start/stop during UK business hours to minimize costs `S`

### Dependencies

- Render account and service configuration
- GitHub repository for auto-deployment
- Persistent disk setup for SQLite database
- UI/UX design for web interface
- Security and authentication implementation

## Phase 4: Future Enhancements (Speculative)

**Goal:** Additional features based on user feedback and business needs
**Status:** 🔮 Future/Speculative

### Phase 4.1: API & Observability Layer

**Priority:** Low (Nice-to-have)
**Use Cases:** Manual testing, system monitoring, third-party integrations

#### Features

- [ ] REST API server - Query database and trigger processes programmatically `L`
  - GET /api/companies - List all strike-off companies
  - GET /api/companies/{id} - Get company details
  - GET /api/companies/{id}/officers - Get company officers
  - POST /api/companies/{id}/import-officers - Trigger officer import
  - GET /api/streaming/status - Check streaming service health
  - POST /api/streaming/process/{id} - Manually trigger company processing
  - GET /api/stats - Database and processing statistics

- [ ] Monitoring dashboard - Visual interface for system health and metrics `M`
  - Real-time streaming status
  - Processing queue visualization
  - Error logs and alerts
  - Database statistics

- [ ] Webhook integrations - Push notifications for key events `S`
  - New strike-off companies detected
  - Companies leaving strike-off status
  - Processing errors or failures

- [ ] Postman collection - Pre-configured API testing suite `S`

#### Dependencies

- FastAPI or Flask framework
- API authentication strategy
- Dashboard framework (React/Vue/Streamlit)
- Webhook delivery infrastructure

#### Rationale

While the current system operates autonomously without requiring external API access, an API layer would provide:
- Easier debugging and manual testing capabilities
- Foundation for future UI development
- Integration points for external systems
- Enhanced observability for production monitoring

This remains low priority as the core system functions well without it, and development efforts are better focused on Apollo.io and GoHighLevel integrations which provide direct business value.

### Phase 4.2: Enterprise-Grade Rate Limiting Resilience

**Priority:** Medium (Risk Mitigation)
**Use Cases:** High-volume periods, economic downturns, mass company failures

#### Problem Statement

During extreme scenarios (economic crisis, regulatory changes, sectoral collapses), streaming events could surge beyond current rate limiting capacity, potentially overwhelming the REST API rate limits (600 requests per 5 minutes) and causing system failures.

#### Features

**Phase 1: Intelligent Queuing System (Foundation)**
- [ ] Priority-based request queue - High priority for status checks, low priority for officer fetching `L`
- [ ] Queue monitoring and metrics - Track queue depth, processing rate, wait times `S`
- [ ] Configurable queue limits - Prevent memory exhaustion during extreme load `S`
- [ ] Queue persistence - Survive service restarts without losing requests `M`

**Phase 2: Circuit Breaker Pattern (Reliability)**
- [ ] API circuit breaker implementation - Fail gracefully when overwhelmed `M`
- [ ] Auto-recovery mechanisms - Detect when conditions improve and resume `M`
- [ ] Degraded mode operations - Store events for later processing when APIs unavailable `L`
- [ ] Circuit breaker metrics and alerting - Operational visibility into system state `S`

**Phase 3: Adaptive Rate Limiting (Optimization)**
- [ ] Dynamic rate limit adjustment - Respond to API response times and error rates `L`
- [ ] Exponential backoff for 429 responses - Handle rate limit exceeded scenarios `M`
- [ ] Self-tuning algorithms - Learn optimal request patterns over time `L`
- [ ] Rate limit coordination - Smart distribution across multiple services `M`

#### Dependencies

- Queue infrastructure (Redis or in-memory)
- Metrics collection system
- Circuit breaker library (e.g., pybreaker)
- Rate limiting algorithms implementation

#### Success Metrics

- **Reliability**: 99.9% uptime during high-volume periods
- **Performance**: Process 10x normal load without data loss
- **Recovery**: Automatic recovery from rate limit violations within 5 minutes
- **Visibility**: Real-time monitoring of queue depth and processing rates

#### Rationale

Following enterprise architecture patterns (Reliability → Observability → Performance → Optimization), this approach ensures the system remains operational during extreme business conditions while providing clear operational visibility. The phased approach allows incremental implementation based on actual operational data and business needs.
