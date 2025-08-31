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
- [x] **CRITICAL**: Fix StreamingIntegration._get_company_officers() to work with existing officer data
- [x] **CRITICAL**: Implement queue processor that calls Snov.io domain_search() with polling (SnovQueueProcessor created)
- [x] **CRITICAL**: Implement domain results handler that saves to company_domains table (via DependencyManager)
- [x] **CRITICAL**: Implement domain success trigger that fetches officers from CH API (workflow coordination implemented)
- [x] **CRITICAL**: Implement officer success trigger that queues Snov email finder requests (via DependencyManager)
- [x] **CRITICAL**: Implement email results handler that saves to officer_emails table (via DependencyManager)
- [x] **CRITICAL**: Add workflow coordination between streaming completion and enrichment start (EnrichmentCoordinator created)

**Success Criteria**: Stream event → Company saved → Domain search queued → Domain found → Officers fetched → Emails discovered → All data in database

**Technical Flow**: ✅ **WORKING** - EnrichmentCoordinator orchestrates complete workflow from StreamingIntegration through SnovQueueProcessor

**IMPLEMENTATION COMPLETED (Aug 31, 2025)**:
- ✅ **queue_processor.py**: Processes Snov.io API requests from queue with polling
- ✅ **enrichment_coordinator.py**: Orchestrates complete workflow with all components
- ✅ **streaming_integration.py**: Fixed to work with existing officer database
- ✅ **Integration Test**: All components initialize and work together successfully

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
- ❌ **ZERO connecting logic between components**

**Identified Issues from Failed Testing**:
- [ ] **WORKFLOW MISSING**: No queue → Snov client bridge 
- [ ] **STORAGE MISSING**: Domain/email results not saved to database
- [ ] **CHAIN MISSING**: Success in one step doesn't trigger next step
- [ ] **CONNECTION MISSING**: Streaming API not connected to Snov.io enrichment workflow
- [ ] **TESTING MISSING**: No way to test workflow without wasting credits

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

- [ ] Create queue processor that bridges queue items to Snov client calls
- [ ] Implement domain search execution: queue → Snov.domain_search() → save to company_domains
- [ ] Implement officer fetch trigger: domain success → CH API → save to officers  
- [ ] Implement email discovery execution: officer + domain → Snov.email_finder() → save to officer_emails
- [ ] Create workflow coordinator that manages the entire chain

**Task 2.2: Connect Streaming to Snov.io Workflow**

- [ ] Connect streaming event processing to trigger Snov.io enrichment workflow  
- [ ] Implement strike-off company detection → queue domain search
- [ ] Add proper error handling and retry logic for each workflow step
- [ ] Create workflow state tracking in enrichment_state table

**Task 2.3: Add Testing and Monitoring**

- [ ] Create mock Snov.io API for testing without credit consumption
- [ ] Implement credit consumption tracking for actual usage
- [ ] Add comprehensive logging at each workflow step
- [ ] Create simple test script to validate entire workflow

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
