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

### Track A: Bulk Data Processing (3-5 days)

- [ ] Process August 15, 2025 bulk data snapshot - Download and import latest BasicCompanyDataAsOneFile CSV `L`
- [ ] Update officer data - Re-run officer fetching for new companies added since May 2025 `L`
- [ ] Database cleanup - Remove companies no longer in strike-off status `M`
- [ ] Performance optimization - Optimize officer fetching to reduce total processing time `M`

### Track B: Real-Time Monitoring (Immediate)

- [ ] Companies House Streaming API registration - Obtain streaming API key separate from REST API `M`
- [ ] Streaming API client implementation - Build HTTP client for long-running connections `L`
- [ ] Company status monitoring - Monitor company information stream for strike-off status changes `L`
- [ ] Officer streaming integration - Monitor officer stream for director changes in tracked companies `M`
- [ ] Real-time officer fetching - Automatically fetch officers for new strike-off companies `L`

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
