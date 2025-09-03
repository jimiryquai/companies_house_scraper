# Snov.io Webhook Workaround

## Issue

The Snov.io v2 API does not accept webhook URLs in POST request payloads. When a webhook URL is included, the API returns an error or rejects the request.

## Discovery

During testing with Postman and the API directly, we discovered that:
1. POST requests with webhook URLs fail
2. The API returns a `task_hash` for async operations
3. Results must be retrieved via polling

## Solution: Polling-Based Approach

Instead of using webhooks, we implement a two-step polling process:

### Step 1: Submit Request Without Webhook
```python
# POST to Snov.io v2 API
response = requests.post(
    "https://api.snov.io/v2/domain-search",
    json={
        "company_name": "Example Ltd",
        # No webhook_url parameter
    },
    headers={"Authorization": f"Bearer {access_token}"}
)

# Extract task_hash from response
task_hash = response.json().get("task_hash")
```

### Step 2: Poll for Results
```python
# GET request with task_hash to retrieve results
def poll_for_results(task_hash, max_attempts=30, delay=10):
    for attempt in range(max_attempts):
        response = requests.get(
            f"https://api.snov.io/v2/get-task-result/{task_hash}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if response.json().get("status") == "completed":
            return response.json().get("data")
        
        time.sleep(delay)
    
    raise TimeoutError("Task did not complete in time")
```

## Implementation Details

### Modified Workflow

1. **Company Domain Search**:
   - POST to `/v2/domain-search` without webhook
   - Store `task_hash` in database with job ID
   - Poll for results asynchronously

2. **Email Finder**:
   - POST to `/v2/email-finder` without webhook
   - Store `task_hash` for polling
   - Retrieve results when ready

### Database Changes

Added `task_hash` column to track async operations:
```sql
ALTER TABLE snov_jobs ADD COLUMN task_hash VARCHAR(255);
ALTER TABLE snov_jobs ADD COLUMN status VARCHAR(50) DEFAULT 'pending';
ALTER TABLE snov_jobs ADD COLUMN last_polled TIMESTAMP;
```

### Webhook Validation Endpoint

Although we don't use webhooks for results, Snov.io may still send GET requests to validate webhook URLs during setup. We maintain a simple validation endpoint:

```python
@app.route('/webhook/snov', methods=['GET'])
def validate_webhook():
    # Snov.io sends GET request to validate webhook URL
    return jsonify({"status": "ok"}), 200
```

## Benefits of This Approach

1. **Reliability**: No dependency on webhook delivery
2. **Simplicity**: Direct polling is easier to debug
3. **Control**: We control polling frequency and timeout
4. **Recovery**: Easy to resume polling after failures

## Configuration

In `config.yaml`:
```yaml
snov_io:
  polling:
    max_attempts: 30
    delay_seconds: 10
    timeout_minutes: 5
```

## Migration from Webhook-Based Design

The original architecture assumed webhook callbacks. Key changes:

1. **Remove webhook_url from all API calls**
2. **Add polling logic to enrichment manager**
3. **Store task_hash for tracking**
4. **Implement background polling service**

## Testing

Test the polling mechanism:
```bash
python tests/snov_integration/test_polling.py
```

## Monitoring

Track polling performance:
- Average completion time
- Number of polling attempts
- Failed polls after timeout
- Credit consumption per successful poll

## Future Considerations

If Snov.io adds webhook support in the future:
1. Keep polling as fallback mechanism
2. Implement hybrid approach (webhook with polling fallback)
3. Gradual migration to webhooks when stable