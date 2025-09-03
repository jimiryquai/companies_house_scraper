# DEFINITIVE WORKFLOW REQUIREMENTS

**CRITICAL: This is the exact workflow that MUST be implemented - no deviations**

## The 5-Step Process (Non-Negotiable)

### Step 1: Companies House Streaming API
- Receives events about company changes
- **DOES NOT** provide `company_status_detail`
- Only tells us "something changed" for a company

### Step 2: Companies House REST API (MANDATORY FOR ALL COMPANIES)
- **URL**: `https://api.company-information.service.gov.uk/company/{companyNumber}`
- **Rate Limit**: 600 calls per 5 minutes
- **Purpose**: Get `company_status_detail` field
- **CRITICAL**: This call MUST be made for EVERY company from Step 1
- **Filter**: Only proceed if `company_status_detail == "active-proposal-to-strike-off"`

### Step 3: Snov.io Domain Search (CONDITIONAL - only if strike-off detected)
- **POST**: `https://api.snov.io/v2/company-domain-by-name/start`
- **GET**: `https://api.snov.io/v2/company-domain-by-name/result?task_hash={hash_from_1}`
- **Credits**: 1 credit consumed
- **Exit Condition**: If no domain found, process ends here

### Step 4: Companies House Officers API (CONDITIONAL - only if domain found)
- **URL**: `https://api.company-information.service.gov.uk/company/{company_number}/officers`
- **Rate Limit**: Same 600 calls per 5 minutes
- **Exit Condition**: If no officers found, process ends here

### Step 5: Snov.io Email Finder (CONDITIONAL - only if officers found)
- **POST**: `https://api.snov.io/v2/emails-by-domain-by-name/start`
- **GET**: `https://api.snov.io/v2/emails-by-domain-by-name/result`
- **Credits**: 1 credit per officer search

## Current Problem

The system is receiving Step 1 events and queuing Step 2 requests, but the CH API processor that should execute Steps 2-5 is either:
1. Not running at all
2. Running but failing silently
3. Running but not processing the queue

## What Needs To Happen Next

1. **Verify CH API processor is actually running and processing requests**
2. **Ensure Step 2 calls are being made to Companies House REST API**
3. **Ensure strike-off detection triggers Steps 3-5**
4. **Ensure actual Snov.io API calls are made and credits consumed**

## Success Criteria

- See CH REST API calls in logs (Step 2)
- See strike-off companies detected
- See Snov.io domain searches (Step 3) 
- See credits consumed in Snov.io dashboard
- See officers fetched (Step 4)
- See email searches (Step 5)

**NO MORE RUNNING IN CIRCLES - IMPLEMENT THIS EXACT WORKFLOW**