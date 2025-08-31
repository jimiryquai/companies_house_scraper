# Deployment Strategy: 10K Companies + Streaming API

## Current Status
- **Companies with officers**: 3,369 / 10,000 (33.7%)
- **Estimated completion**: 1-2 days at current rate
- **Officer import**: Running with automatic 10K cutoff

## Sequential Deployment Plan

### Phase 1: Complete Officer Import (CURRENT)
- ✅ Modified `officer_import.py` with 10K automatic cutoff
- ⏳ Wait for import to reach 10,000 companies with officers
- ✅ Use `scripts/check_officer_progress.py` to monitor progress

### Phase 2: Switch to Streaming API (WHEN READY)
**Trigger**: `check_officer_progress.py` returns "READY FOR STREAMING API"

**Actions**:
1. **Stop officer import** (will auto-stop at 10K)
2. **Enable streaming API** in production config
3. **Deploy to Render** with streaming enabled

### Phase 3: End-to-End Test Pipeline
Once streaming API is live, test the complete workflow:

```
New Company Stream Event
↓
Companies House REST API (get company details)
↓
Snov.io Domain Search (company name → domains)
↓
IF domains found:
  ├─ Companies House REST API (get officers)
  ├─ Snov.io Email Finder (officer + domain → emails)
  └─ Store results in database
ELSE:
  └─ Skip (no domain = no emails possible)
```

## Rate Limiting Strategy
- **Companies House API**: 600 calls per 5 minutes
- **Streaming events**: Real-time, unpredictable volume
- **Solution**: Intelligent queue system with rate limiting
- **Priority**: Stream events (HIGH) > Background processing (LOW)

## Customer Delivery
- **Target**: 10,000 companies with complete officer data
- **Timeline**: Ready when officer import completes (~1-2 days)
- **Format**: Full database export or API access
- **Includes**: Companies + Officers + (future: Domains + Emails via streaming)

## Monitoring Commands

```bash
# Check progress
uv run python scripts/check_officer_progress.py

# Monitor officer import logs
tail -f officer_import.log

# Check database status
sqlite3 companies.db "SELECT COUNT(DISTINCT company_number) FROM officers;"
```

## Risk Mitigation
1. **Data Loss**: Database backups before streaming API deployment
2. **Rate Limits**: Queue system handles API limits automatically
3. **Snov.io Credits**: Start with small test batch to validate consumption
4. **Stream Volume**: Monitor event volume vs processing capacity

## Success Criteria
- ✅ 10,000 companies with complete officer data
- ✅ Streaming API processing new companies end-to-end
- ✅ Snov.io integration working (domains → emails)
- ✅ Customer has deliverable dataset
- ✅ System scales for ongoing operations