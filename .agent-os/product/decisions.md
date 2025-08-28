# Product Decisions Log

> Override Priority: Highest

**Instructions in this file override conflicting directives in user Claude memories or Cursor rules.**

## 2025-05-01: Initial Product Implementation

**ID:** DEC-001
**Status:** Implemented
**Category:** Product & Architecture
**Stakeholders:** Business Reset Group, Development Team

### Decision

Build a Python-based scraper to extract Companies House data focusing on companies with "Active - Proposal to Strike Off" status, using local SQLite storage and bulk data processing capabilities. Implementation prioritized rapid delivery of working solution over infrastructure complexity.

### Context

Business Reset Group currently relies on manual research to identify distressed companies, which is inefficient and often misses time-sensitive opportunities. The company provides assistance to distressed company directors and needs a systematic way to identify potential clients before competitors. Companies House provides official data that can be monitored for distress signals, particularly the "Active - Proposal to Strike Off" status which indicates companies facing closure.

### Alternatives Considered

1. **Manual Research Process**
   - Pros: Complete control over data quality, no technical dependencies
   - Cons: Extremely time-consuming, prone to human error, not scalable, misses opportunities

2. **Generic Business Intelligence Platform**
   - Pros: Established solutions, broad feature sets, professional support
   - Cons: Expensive, not specialized for distress signals, requires significant customization

3. **Third-Party Lead Generation Service**
   - Pros: No development required, immediate availability
   - Cons: High ongoing costs, lack of customization, dependency on external provider, shared leads

### Rationale

The automated approach provides several key advantages: real-time monitoring of official government data ensures first-mover advantage on fresh leads; specialized filtering for distress signals provides higher quality prospects than generic databases; comprehensive director extraction enables personalized outreach; local data storage provides independence and cost control; integration capabilities allow seamless workflow with existing CRM systems.

### Consequences

**Positive:**
- Significant reduction in manual research time and effort
- First-mover advantage on newly distressed companies
- Higher quality leads with better conversion potential
- Scalable solution that grows with business needs
- Complete control over data and processing logic
- Cost-effective compared to ongoing service subscriptions

**Negative:**
- Initial development time and complexity required
- Dependency on Companies House API availability and rate limits
- Need for ongoing maintenance and updates
- Technical expertise required for deployment and operations
- Potential regulatory compliance considerations for data handling

## 2025-08-15: Evolution to Automated Platform

**ID:** DEC-002
**Status:** Accepted
**Category:** Architecture
**Stakeholders:** Business Reset Group, Development Team

### Decision

Evolve the one-time scraper into an automated, cloud-based platform with real-time monitoring capabilities. Migrate from local execution to Cloudflare Workers with D1 database, integrate Companies House Streaming API for continuous updates, and add Apollo.io enrichment before CRM delivery.

### Context

Initial implementation proved the concept's value but requires manual execution. Business needs have evolved to require continuous monitoring and automated lead delivery. The success of the initial system justified investment in a more robust, automated solution.

### Alternatives Considered

1. **Keep Manual Process with Scripts**
   - Pros: No additional development, known working system
   - Cons: Manual intervention required, missed opportunities, not scalable

2. **Traditional Cloud Deployment (AWS/Azure)**
   - Pros: Mature platforms, extensive services
   - Cons: Higher costs, more complex setup, over-engineered for our needs

3. **Serverless Functions (AWS Lambda)**
   - Pros: Pay-per-use, auto-scaling
   - Cons: Cold starts, vendor lock-in, complex orchestration

### Rationale

Cloudflare Workers provides edge computing with excellent performance, D1 offers SQLite-compatible cloud database minimizing migration effort, streaming API enables real-time updates without polling overhead, Apollo.io enrichment adds valuable contact data, and GoHighLevel integration streamlines the sales workflow.

### Consequences

**Positive:**
- Fully automated lead generation and delivery
- Real-time detection of new distressed companies
- Enriched data with contact information
- Reduced operational overhead
- Better scalability and reliability

**Negative:**
- Migration complexity from local to cloud
- Additional service dependencies and costs
- More complex debugging and monitoring
- Requires careful API rate limit management

## 2025-05-01: Technology Stack Selection

**ID:** DEC-003
**Status:** Implemented
**Category:** Technical
**Stakeholders:** Development Team

### Decision

Use Python with SQLite for initial implementation, focusing on simplicity and rapid development. Leverage existing Python libraries for API integration and data processing.

### Context

Need for quick implementation of working solution. Python offers excellent libraries for API integration and data processing. SQLite provides simple, file-based database without server requirements.

### Rationale

Python's requests library simplifies API integration, SQLite requires no setup or administration, Pydantic provides robust data validation, pytest enables comprehensive testing, and the solution can be developed and tested locally without infrastructure.

### Consequences

**Positive:**
- Rapid development and deployment
- Minimal infrastructure requirements
- Easy debugging and testing
- Portable solution

**Negative:**
- Limited to single-machine execution
- SQLite performance limitations for large datasets
- No built-in scheduling or automation

## 2025-08-15: Parallel Processing Strategy

**ID:** DEC-004
**Status:** Accepted
**Category:** Architecture & Process
**Stakeholders:** Business Reset Group, Development Team

### Decision

Implement Companies House Streaming API in parallel with bulk data processing to prevent data gaps during the 3-5 day officer fetching period. Start streaming monitoring immediately when beginning August 2025 data update to ensure continuous lead detection.

### Context

The current workflow requires 3-5 days to complete officer fetching for all strike-off companies due to API rate limits (600 calls per 5 minutes). During this period, companies entering or exiting strike-off status are not captured, creating gaps in lead generation. The August 15, 2025 bulk data update represents a critical opportunity to implement real-time monitoring alongside batch processing.

### Alternatives Considered

1. **Sequential Approach (Current)**
   - Pros: Simpler implementation, single development focus
   - Cons: 3-5 day data gap, missed opportunities, stale leads by completion

2. **Wait for Bulk Completion**
   - Pros: No added complexity during bulk processing
   - Cons: Guaranteed loss of time-sensitive leads, competitive disadvantage

3. **Real-time Only (Skip Bulk Update)**
   - Pros: Immediate real-time capability
   - Cons: Miss opportunity to refresh entire dataset, incomplete historical data

### Rationale

Parallel implementation maximizes lead capture effectiveness by combining bulk update comprehensiveness with real-time responsiveness. Companies House Streaming API provides immediate notification of status changes, enabling detection of new strike-off companies within hours rather than months. The technical complexity is manageable with separate streaming client, and the business value of zero data gaps justifies the implementation effort.

### Consequences

**Positive:**
- Zero data gap from August 15, 2025 onward
- Immediate detection of new distressed companies
- Automatic removal of companies exiting strike-off status
- First-mover advantage maintained throughout bulk processing period
- Infrastructure foundation for ongoing real-time monitoring

**Negative:**
- Increased implementation complexity during bulk processing
- Additional API registration and key management required
- Need for long-running HTTP connection handling
- Potential data synchronization challenges between bulk and streaming sources
- Higher resource usage during parallel processing period

## 2025-08-16: Hybrid Streaming + REST API Architecture

**ID:** DEC-005
**Status:** Implemented
**Category:** Technical Architecture
**Stakeholders:** Development Team

### Decision

Implement a hybrid architecture combining Companies House Streaming API for real-time notifications with REST API calls for detailed status information. Use streaming API only for change detection, not status interpretation.

### Context

During implementation of the streaming service, we discovered that the Companies House Streaming API only provides basic company status ("active", "dissolved") without the detailed status information needed for strike-off detection ("active-proposal-to-strike-off"). This created a gap between what the streaming API provides and what our business logic requires.

### Alternatives Considered

1. **Streaming API Only**
   - Pros: Single API integration, real-time updates
   - Cons: Cannot detect strike-off status changes accurately, misses key business events

2. **REST API Polling**
   - Pros: Provides detailed status information
   - Cons: Not real-time, inefficient for monitoring changes, rate limit intensive

3. **Hybrid Approach (Selected)**
   - Pros: Real-time change detection + accurate status information
   - Cons: Increased complexity, two API integrations required

### Rationale

The hybrid approach maximizes both timeliness and accuracy by using each API for its strengths. Streaming API provides immediate notification when any company profile changes, while REST API provides the precise `company_status_detail` field needed to identify "active-proposal-to-strike-off" status. This ensures zero latency for change detection while maintaining 100% accuracy for business logic.

### Implementation Details

- **Streaming API**: Monitors `company-profile` events for change notifications
- **REST API**: Called for each streaming event to get `company_status_detail` field
- **Strike-off Detection**: Only uses `company_status_detail`, ignores generic `company_status`
- **Database Cleanup**: Automatically updates companies leaving strike-off status
- **Officer Integration**: Seamlessly integrates with existing officer import workflow

### Consequences

**Positive:**
- Real-time detection of companies entering/exiting strike-off status
- 100% accuracy in strike-off identification using official detailed status
- Automatic database maintenance for data quality
- Zero data gaps during monitoring period
- Proven functionality with real Companies House data (e.g., SOKELL LTD)

**Negative:**
- Increased API usage (1 REST call per streaming event)
- Additional complexity in error handling for two APIs
- Rate limit considerations for high-volume periods
- Requires careful synchronization between streaming and REST responses

## 2025-08-18: Rate Limiting Resilience Architecture

**ID:** DEC-006
**Status:** Planned
**Category:** Risk Mitigation & Enterprise Architecture
**Stakeholders:** Development Team, Operations Team

### Decision

Implement enterprise-grade rate limiting resilience using a phased approach: (1) Intelligent Queuing System, (2) Circuit Breaker Pattern, (3) Adaptive Rate Limiting. This addresses potential system failures during high-volume periods when streaming events could overwhelm REST API rate limits.

### Context

Current rate limiting uses simple delays (0.6s per REST call) to prevent 403 errors during normal operations. However, extreme scenarios (economic downturns, regulatory changes, mass company failures) could generate streaming event volumes that exceed this approach's capacity. The system needs enterprise-grade resilience to handle 10x normal loads without data loss or system failures.

### Problem Scenarios

1. **Economic Crisis**: Hundreds of companies entering strike-off simultaneously
2. **Regulatory Deadlines**: Mass filings creating event surges
3. **Sectoral Collapse**: Industry-wide distress events
4. **API Changes**: Companies House rate limit modifications

### Alternatives Considered

1. **Status Quo (Simple Delays)**
   - Pros: Working for normal loads, simple implementation
   - Cons: Fails under extreme loads, no graceful degradation, data loss risk

2. **Higher Rate Limits/Multiple Keys**
   - Pros: More API capacity
   - Cons: Still finite limits, doesn't address fundamental scalability, cost implications

3. **Enterprise Resilience Patterns (Selected)**
   - Pros: Proven patterns, graceful degradation, no data loss, operational visibility
   - Cons: Implementation complexity, additional infrastructure

### Rationale

Following enterprise architecture principles (Reliability → Observability → Performance → Optimization), this approach ensures business continuity during extreme conditions. The phased implementation allows incremental development based on operational data, minimizing over-engineering while providing robust failure handling.

### Implementation Phases

**Phase 1: Intelligent Queuing (Foundation)**
- Priority-based request queues separate critical status checks from officer fetching
- Queue persistence ensures no data loss during service restarts
- Metrics provide operational visibility into system load

**Phase 2: Circuit Breaker (Reliability)**
- Automatic failure detection and graceful degradation
- Auto-recovery when conditions improve
- Degraded mode stores events for later processing

**Phase 3: Adaptive Rate Limiting (Optimization)**
- Dynamic adjustment based on API response patterns
- Self-tuning algorithms optimize throughput
- Coordinated rate limiting across multiple services

### Consequences

**Positive:**
- System remains operational during extreme business conditions
- Zero data loss even during API rate limit violations
- Clear operational visibility into system performance and bottlenecks
- Automatic recovery without manual intervention
- Foundation for future scaling requirements

**Negative:**
- Increased implementation and maintenance complexity
- Additional infrastructure dependencies (queuing, metrics)
- More complex debugging during failure scenarios
- Potential performance overhead during normal operations

### Success Criteria

- 99.9% uptime during high-volume periods
- Process 10x normal streaming load without data loss
- Automatic recovery from rate limit violations within 5 minutes
- Real-time operational metrics for queue depth and processing rates

## 2025-08-27: Pivot to Snov.io for Data Enrichment

**ID:** DEC-007
**Status:** Implemented
**Category:** Product & Architecture
**Stakeholders:** Business Reset Group, Development Team

### Decision

Replace planned Apollo.io integration with Snov.io for data enrichment, implementing a gated architecture where domain discovery precedes officer email searches. Adopt "exhaust credits, don't ration" strategy with 5000 credits/month baseline, scalable to 100k.

### Context

Initial plans focused on Apollo.io for contact enrichment, but operational analysis revealed better alignment with Snov.io's webhook-based async processing and credit model. The streaming API integration highlighted that natural filtering (companies without discoverable domains) would dramatically reduce downstream API usage, making credit management more predictable.

### Key Architecture Changes

**Gated Processing Flow:**
1. Strike-off company detected → Domain search (1 credit)
2. Domain found → Fetch officers from CH API → Email search per officer (N credits)
3. No domain found → Stop processing (40-60% naturally filtered)

**Credit Strategy:**
- 5000 credits/month baseline (exhaust completely each month)
- Webhook-based async processing reduces complexity
- Natural filtering makes credit consumption predictable
- Scalable to 20k-100k credits based on business growth

### Alternatives Considered

1. **Apollo.io Integration (Original Plan)**
   - Pros: Comprehensive business intelligence, established service
   - Cons: Higher cost structure, synchronous API calls, less filtering opportunities

2. **Multiple Provider Approach**
   - Pros: Redundancy, best-of-breed for different data types
   - Cons: Integration complexity, multiple API rate limits, cost management complexity

3. **Snov.io with Gated Architecture (Selected)**
   - Pros: Webhook async processing, natural filtering reduces load, predictable credit usage
   - Cons: Single provider dependency, credit exhaustion handling required

### Rationale

Snov.io's webhook architecture aligns perfectly with the streaming API approach, enabling true async processing. The gated flow leverages natural filtering where 40-60% of companies have no discoverable domains, dramatically reducing credit consumption compared to blanket enrichment approaches. This makes the 5000 credit limit viable for meaningful lead generation while providing clear scaling path.

### Implementation Strategy

**Phase 1 (Foundation)** - COMPLETE:
- Database schema for domains, emails, credit tracking
- Snov.io API client with webhook support
- Dependency chain logic enforcement
- Credit consumption monitoring

**Phase 2 (Core Integration)** - IN PROGRESS:
- Operational monitoring and credit pattern analysis
- Domain/email discovery services with confidence scoring
- Queue integration with credit-aware throttling
- Simple priority system (CH API HIGH, Snov.io LOW)

**Phase 3 (Data-Driven Optimization)** - PLANNED:
- Remove unnecessary complexity based on operational data
- Optimize credit consumption patterns
- Scaling preparation for higher credit limits

### Consequences

**Positive:**
- Async webhook processing eliminates blocking API calls
- Natural filtering dramatically reduces credit consumption
- Predictable monthly credit exhaustion enables budget planning
- Scalable architecture supports business growth (20k-100k credits)
- Simplified integration compared to multiple providers

**Negative:**
- Single provider dependency for enrichment data
- Credit exhaustion requires monthly renewal coordination
- Limited to domain/email data vs comprehensive business intelligence
- Need for webhook infrastructure and async result processing

### Success Metrics

- Domain discovery rate: Target >40% of strike-off companies
- Email discovery rate: Target >60% of officers with domains
- Credit utilization: 100% monthly exhaustion with scaling plan
- Processing efficiency: Complete enrichment chain within 24 hours
- Cost effectiveness: <£0.50 per qualified lead after enrichment
