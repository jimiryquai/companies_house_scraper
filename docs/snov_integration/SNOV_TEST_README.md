# Snov.io Integration Test

Quick test setup to validate Snov.io integration with 250 companies using 500 test credits.

## Quick Start

### Terminal 1: Start Services
```bash
./run_snov_test.sh
```
This will:
- Start ngrok tunnel (exposes localhost:5000 publicly)
- Start Flask webhook handler
- Display the webhook URL to use

### Terminal 2: Run Test
```bash
python3 test_snov_250_companies.py
```
- Enter the webhook URL from Terminal 1 when prompted
- Confirm to start processing 250 companies

### Terminal 3: Monitor Results (Optional)
```bash
# Check real-time stats
curl http://localhost:5000/stats

# Or check via ngrok URL
curl https://your-ngrok-url.ngrok-free.app/stats
```

## What Happens

1. **Domain Discovery**: Script calls Snov.io API for 250 companies
2. **Webhook Results**: Results arrive asynchronously via webhook
3. **Database Storage**: All results saved to database tables
4. **Credit Tracking**: Usage monitored in real-time

## Expected Timeline

- **API Calls**: ~8 minutes (250 companies × 2s delay)
- **Webhook Results**: 2-4 hours (async processing)
- **Total Credits**: ~250 credits (1 per domain search)

## Analyzing Results

After webhook results arrive, run:
```bash
python3 analyze_test_results.py
```

This generates:
- Stakeholder report (markdown)
- Raw data export (JSON)
- Success rate analysis
- Credit consumption patterns

## Test Files

- `test_webhook_handler.py` - Receives Snov.io webhooks
- `test_snov_250_companies.py` - Main test script
- `analyze_test_results.py` - Results analysis
- `run_snov_test.sh` - Startup coordinator

## Monitoring

- **Health Check**: `GET /health`
- **Live Stats**: `GET /stats`
- **Webhook Logs**: Check Terminal 1 output

## Troubleshooting

### ngrok Issues
- Make sure `./ngrok` binary exists
- Check ngrok dashboard: http://localhost:4040
- Ensure webhook URL starts with `https://`

### Webhook Issues
- Check Flask app is running on port 5000
- Verify ngrok tunnel is active
- Check webhook handler logs for errors

### Database Issues
- Ensure `companies.db` exists with strike-off companies
- Check database schema has required tables
- Verify database permissions

## Sample Expected Results

Based on typical performance:
- **Domain Success Rate**: 20-40%
- **Email Success Rate**: 60-80% (of companies with domains)
- **Credits Used**: ~250 for domain searches
- **Processing Time**: 2-4 hours for complete results

## Next Steps

After test completion:
1. Review stakeholder report
2. Analyze success rates and credit efficiency
3. Use data to validate production architecture
4. Scale to full 5000 credit monthly allocation
