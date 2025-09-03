# Spec Tasks

These are the tasks to be completed for the spec detailed in @.agent-os/specs/2025-08-27-snov-io-integration/spec.md

> Created: 2025-08-27
> Status: Ready for Implementation
> Updated: 2025-08-27 - Revised based on operational data-driven approach and credit constraints

## Key Changes Made

**Philosophy Shift**: From complex upfront architecture to **operational data-driven development**

**Major Updates**:
- **Phase 1**: Focus on essential integration components and data collection
- **Phase 2**: Prioritize operational monitoring and basic functionality
- **Phase 3**: Data-driven simplification and optimization
- **Phase 4-5**: Mark advanced features as "EVALUATE" based on real usage

**Credit Constraint Focus**: Emphasize webhook efficiency, dependency chain logic, and credit exhaustion handling over complex retry mechanisms.

**Removed Over-Engineering**: Simplified complex retry logic, reduced priority levels, made advanced monitoring optional.

**Recent Completions (Aug 31, 2025)**:
- [x] **Repository Cleanup**: Organized messy root directory structure into tests/, scripts/, logs/, docs/
- [x] **Database Schema Migration**: Applied all migrations including Snov.io tables (company_domains, officer_emails, snov_credit_usage, snov_webhooks)
- [x] **Webhook Polling Workaround**: Implemented polling mechanism in Snov client (awaiting Snov support response on webhook fix)
- [x] **Schema Consistency**: Fixed credits_consumed vs credits_used inconsistencies across codebase
- [x] **Officer Import Cutoff**: Modified officer import to automatically stop at 10,000 companies for customer delivery

**CURRENT REALITY CHECK (Aug 31, 2025)**:
❌ **Previous task markings were misleading** - many items marked [x] were just infrastructure/placeholders, NOT working functionality
✅ **What actually works**: Database schema, Snov client with polling, officer import with cutoff, streaming API implementation
❌ **What doesn't work**: Snov.io integration workflow (domain search → officer fetch → email discovery)
🔄 **Webhook Strategy**: Currently using polling workaround pending Snov.io support response
   - **If Snov fixes webhooks**: May revert to webhook implementation (fewer API calls, more efficient)
   - **Architecture designed for flexibility**: Can switch between polling and webhooks with minimal changes
   - **Current priority**: Get polling workflow working first, optimize method later

## Tasks

### IMMEDIATE PRIORITY: Make E2E Workflow Actually Work

**Current Blocker**: Infrastructure exists but no connecting logic between components

**SPECIFIC TECHNICAL ISSUE IDENTIFIED (Aug 31, 2025)**:
- [x] **CRITICAL FIX**: StreamingIntegration._get_company_officers() method (line 337-348) returns empty list instead of queuing officers from CH REST API
- [x] **ROOT CAUSE**: Method now queries database for existing officers, returns empty list if none found (allows system to continue)
- [x] **IMPACT**: Streaming API can now process companies with officers in database, skips others gracefully

**Required for E2E Test**:
- [x] **CRITICAL**: Fix StreamingIntegration to use domain-first REST API workflow (corrected from database shortcut)
- [x] **CRITICAL**: Implement queue processor that calls Snov.io domain_search() with polling (SnovQueueProcessor created)
- [x] **CRITICAL**: Implement domain results handler that saves to company_domains table (via DependencyManager._save_company_domains)
- [x] **CRITICAL**: Implement domain success trigger that fetches officers from CH API (workflow coordination implemented)
- [x] **CRITICAL**: Implement officer success trigger that queues Snov email finder requests (via DependencyManager._queue_officer_email_discovery)
- [x] **CRITICAL**: Implement email results handler that saves to officer_emails table (via DependencyManager.handle_officer_email_completion)
- [x] **CRITICAL**: Add workflow coordination between streaming completion and enrichment start (EnrichmentCoordinator created)

**Success Criteria**: Stream event → Company saved → Domain search queued → Domain found → Officers fetched → Emails discovered → All data in database

**Technical Flow**: ⚠️ **COMPONENTS IMPLEMENTED** - EnrichmentCoordinator orchestrates workflow, but needs E2E testing with real CH API calls

**IMPLEMENTATION STATUS (Aug 31, 2025)**:
- ✅ **queue_processor.py**: Processes Snov.io API requests from queue with polling
- ✅ **enrichment_coordinator.py**: Orchestrates complete workflow with all components  
- ✅ **streaming_integration.py**: Fixed to use domain-first REST API workflow (not database shortcuts)
- ✅ **dependency_manager.py**: Added proper async coordination methods for officers fetch completion
- ⚠️ **Integration Test**: Components initialize successfully, but E2E workflow needs testing with real API calls

### Phase 1: Foundation (Week 1) - **INFRASTRUCTURE COMPLETED**

**Task 1.1: Database Schema Implementation**

- [x] Create migration files for new tables (company_domains, officer_emails, snov_credit_usage, snov_webhooks)
- [x] Extend queue_jobs table with Snov.io specific columns
- [x] Run migrations and verify schema changes
- [x] Create database indexes for performance optimization
- [x] Test database schema with sample data

**Task 1.2: Snov.io API Client Development**

- [x] Complete base Snov.io API client (already exists, verify functionality)
- [x] Implement Domain Search API client method with webhook support
- [x] Implement Email Finder API client method with webhook support
- [x] Add simple error handling and basic retry logic (no exponential backoff)
- [x] Create unit tests for API client methods
- [ ] **CRITICAL FIX**: Update API base URL from v1 to v2 for bulk operations
- [ ] **CRITICAL FIX**: Add missing bulk API methods (emails-by-domain-by-name, bulk-task-result)
- [ ] **CRITICAL FIX**: Fix OAuth token handling for v2 endpoints

**Task 1.2.1: Fix Credit Balance API Integration**
- [x] **CRITICAL**: Update balance check from `account-info` to `get-balance` endpoint (current returns 404)
- [x] **CRITICAL**: Remove incorrect `credits_consumed` tracking from API responses (field doesn't exist)
- [x] **CRITICAL**: Implement operation-based credit tracking (count operations × known costs)
- [ ] **CRITICAL**: Test `GET https://api.snov.io/v1/get-balance` endpoint with real credentials
- [x] **CRITICAL**: Update `_check_credits()` method to use correct endpoint
- [x] **CRITICAL**: Fix credit consumption recording to use operation counting instead of response parsing

**Task 1.2.2: Implement Credit-Aware Workflow Orchestra**
- [ ] **CRITICAL**: Create credit-aware workflow coordinator that checks balance before each Snov.io operation
- [ ] **CRITICAL**: Implement pre-operation credit checking (before domain search, before email search)
- [ ] **CRITICAL**: Implement post-operation credit tracking (after domain search, after email search)
- [ ] **CRITICAL**: Add credit exhaustion handling that stops Snov.io operations but continues CH API
- [ ] **CRITICAL**: Update workflow to use v2 endpoints for domain/email operations
- [ ] **CRITICAL**: Ensure CH REST API continues regardless of credit status (non-blocking)
- [ ] **CRITICAL**: Add actual credit consumption recording (may be 0 if no results found)

**Task 1.2.3: Fix Snov.io API v2 Integration**
- [ ] **CRITICAL**: Update domain search to use v2 endpoints (`POST /v2/company-domain-by-name/start`)
- [ ] **CRITICAL**: Update email search to use v2 endpoints (`POST /v2/emails-by-domain-by-name/start`)
- [ ] **CRITICAL**: Implement v2 result polling (`GET /v2/company-domain-by-name/result?task_hash=`)
- [ ] **CRITICAL**: Fix OAuth token handling for v2 endpoints (may require different authentication)
- [ ] **CRITICAL**: Update polling methods to handle v2 response formats
- [ ] **CRITICAL**: Test v2 endpoints with real credentials to verify they work

**Task 1.3: Essential Integration Components**

- [x] Create webhook endpoint for receiving Snov.io results *(Infrastructure only - not used due to polling workaround)*
- [ ] ~~Fix webhook signature verification~~ *(DEPRIORITIZED - using polling)*
- [x] ~~Fix database schema mismatch~~ *(COMPLETED - credits_consumed fixed)*
- [ ] **CRITICAL MISSING**: Implement actual dependency chain logic (queue → Snov API → database → next step)
- [ ] **CRITICAL MISSING**: Connect enrichment state tracking to actual workflow execution
- [x] ~~Fix webhook URL validation~~ *(COMPLETED - GET endpoint for Snov validation)*

**Task 1.4: Data Models and Schemas**

- [x] Create Pydantic models for Snov.io API responses
- [x] Create database ORM models for new tables
- [x] Implement data validation and serialization
- [x] Create model relationships and foreign key constraints
- [x] Add type hints and documentation
- [ ] **CRITICAL FIX**: Update API response models to match actual Snov.io webhook format

### Phase 1.5: Current Reality - Workflow Implementation Gap

**Root Cause**: Testing last Thursday revealed infrastructure exists but workflow execution is completely missing

**Current State**:
- [x] Database schema ready
- [x] Snov client with polling ready  
- [x] Queue system ready
- ✅ **CONNECTING LOGIC IMPLEMENTED** - Components now properly connected via async coordination

**Implementation Status from Current Work**:
- [x] **WORKFLOW BRIDGE**: Queue → Snov client bridge implemented (SnovQueueProcessor)
- [x] **STORAGE LAYER**: Domain/email results saved to database (DependencyManager helpers)
- [x] **CHAIN COORDINATION**: Success in one step triggers next step (async completion handlers)
- [x] **STREAMING CONNECTION**: Streaming API connected to Snov.io enrichment workflow (StreamingIntegration)
- [ ] **E2E TESTING**: Need to test complete workflow with real API calls (no credit waste)

**Note**: Previous "0% success rate" was due to missing workflow implementation, NOT API issues

**Task 1.6: Testing Infrastructure Improvements**

- [ ] Create isolated testing environment to avoid wasting production credits
- [ ] Implement database transaction rollback for failed tests
- [ ] Add comprehensive unit tests for webhook handler before API integration
- [ ] Create mock Snov.io API for testing without credit consumption
- [ ] Add database schema validation tests
- [ ] Implement proper testing workflow: test → validate → then production

### Phase 2: Make It Actually Work - **THESE ARE THE REAL TASKS**

**Task 2.1: Implement Workflow Execution Layer**

- [x] Create queue processor that bridges queue items to Snov client calls (SnovQueueProcessor)
- [x] Implement domain search execution: queue → Snov.domain_search() → save to company_domains (_save_company_domains)
- [x] Implement officer fetch trigger: domain success → CH API → save to officers (handle_domain_completion → _queue_officers_fetch_after_domain_success)
- [x] Implement email discovery execution: officer + domain → Snov.email_finder() → save to officer_emails (_queue_officer_email_discovery)
- [x] Create workflow coordinator that manages the entire chain (EnrichmentCoordinator)

**Task 2.2: Connect Streaming to Snov.io Workflow**

- [x] Connect streaming event processing to trigger Snov.io enrichment workflow (StreamingIntegration.handle_strike_off_company_enrichment)
- [x] Implement strike-off company detection → queue domain search (initiate_domain_search_first)
- [x] Add proper error handling and retry logic for each workflow step (try/catch blocks with logging)
- [x] Create workflow state tracking in enrichment_state table (EnrichmentStateManager integration)

**Task 2.3: Add Testing and Monitoring**

- [ ] Create mock Snov.io API for testing without credit consumption
- [ ] Implement credit consumption tracking for actual usage
- [ ] Add comprehensive logging at each workflow step
- [ ] Create simple test script to validate entire workflow

**Task 2.3.1: Credit Tracking Validation**
- [ ] **VALIDATION**: Test real-time balance checking with corrected `get-balance` endpoint
- [ ] **VALIDATION**: Verify operation-based credit tracking accuracy vs actual Snov.io consumption
- [ ] **VALIDATION**: Test credit exhaustion handling (when balance reaches 0)
- [ ] **VALIDATION**: Ensure CH REST API continues regardless of Snov.io credit status
- [ ] **VALIDATION**: Test credit allocation scaling (5k → 20k → 50k → 100k)
- [ ] **VALIDATION**: Document actual credit costs per operation type (domain search, email finder)

**Task 2.3.2: 10-Step Credit-Aware Workflow E2E Validation**
- [ ] **E2E TEST**: Validate Step 1-3: CH Streaming → CH REST API → strike-off filter
- [ ] **E2E TEST**: Validate Step 4: Credit check before domain search (`GET /v1/get-balance`)
- [ ] **E2E TEST**: Validate Step 5-6: Domain search (v2 API) + post-operation credit tracking
- [ ] **E2E TEST**: Validate Step 7: Officer fetch from CH REST API (if domain found)
- [ ] **E2E TEST**: Validate Step 8: Credit check before email search (`GET /v1/get-balance`)
- [ ] **E2E TEST**: Validate Step 9-10: Email search (v2 API) + final credit tracking
- [ ] **E2E TEST**: Test workflow with credits = 0 (should stop Snov.io, continue CH API)
- [ ] **E2E TEST**: Test workflow with operations that find no results (may consume 0 credits)
- [ ] **E2E TEST**: Verify complete workflow saves data to all tables (companies, domains, officers, emails, credit_usage)

### Phase 3: Optimization and Enhancement (Week 3)

**Task 3.1: System Simplification Based on Operational Data**

- [ ] Analyze operational metrics from Phase 2 implementation
- [ ] Identify and remove unnecessary complexity from retry/recovery systems
- [ ] Simplify monitoring and alerting based on actual needs
- [ ] Optimize queue system based on real usage patterns
- [ ] Remove over-engineered components that aren't providing value
- [ ] Document architectural decisions and rationale

**Task 3.2: Credit Management Enhancement**

- [ ] Implement credit exhaustion handling and recovery
- [ ] Add credit threshold monitoring and alerts
- [ ] Create credit renewal automation
- [ ] Optimize credit consumption based on operational data
- [ ] Add credit usage analytics and reporting
- [ ] Implement scaling preparation for higher credit limits (20k-100k)

### Phase 4: Advanced Features (Week 4) - **Evaluate After Operational Data**

**Task 4.1: Advanced Caching and Performance (Optional)**

- [ ] **EVALUATE**: Implement intelligent caching if needed based on operational data
- [ ] **EVALUATE**: Add cache TTL management if caching is implemented
- [ ] **EVALUATE**: Optimize database queries if performance issues identified
- [ ] **EVALUATE**: Implement advanced performance monitoring if basic isn't sufficient
- [ ] **EVALUATE**: Create performance benchmarking if scaling issues arise

**Task 4.2: Advanced Health Monitoring (Optional)**

- [ ] **EVALUATE**: Add advanced health checks if basic monitoring insufficient
- [ ] **EVALUATE**: Create monitoring for discovery success rates if needed
- [ ] **EVALUATE**: Implement automated error recovery if manual recovery insufficient
- [ ] **EVALUATE**: Add comprehensive audit trails if regulatory/debugging needs arise

**Task 4.3: Integration Testing and Documentation**

- [ ] Create end-to-end integration tests
- [ ] Test full domain-to-email discovery workflow
- [ ] Validate webhook processing and data consistency
- [ ] Create API documentation with examples
- [ ] Add configuration documentation and setup guides
- [ ] **EVALUATE**: Perform load testing if scaling beyond current limits

### Phase 5: Production Deployment (Week 5)

**Task 5.1: Production Readiness**

- [ ] Add environment configuration for Snov.io credentials
- [ ] Implement secure credential management
- [ ] Configure production webhook endpoints with proper security
- [ ] Set up basic monitoring and alerting based on Phase 2 findings
- [ ] Create operational runbooks for credit management and troubleshooting
- [ ] Validate production deployment functionality

**Task 5.2: Documentation and Scaling Preparation**

- [ ] Document operational procedures and troubleshooting guides
- [ ] Create configuration documentation and setup guides
- [ ] Document architectural decisions and simplification rationale
- [ ] Prepare scaling documentation for 20k-100k credit limits
- [ ] Create maintenance procedures for webhook and credit management
- [ ] Complete project documentation and handover

**Task 5.3: Future Enhancement Planning**

- [ ] Document lessons learned from operational data collection
- [ ] Create roadmap for scaling to higher credit limits
- [ ] Identify components for future enhancement vs removal
- [ ] Plan integration with additional data sources if needed
- [ ] Document framework for future API integrations
- [ ] Create recommendations for system evolution
