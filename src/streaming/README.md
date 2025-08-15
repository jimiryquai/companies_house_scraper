# Companies House Streaming API Module

## Overview

The streaming module provides real-time monitoring of company status changes through the Companies House Streaming API. It processes events as they occur, updating the database with the latest company information within seconds of changes being registered.

## Features

- **Real-time Event Processing**: Connects to Companies House Streaming API for instant updates
- **Automatic Reconnection**: Handles connection failures with exponential backoff and jitter
- **Data Consistency**: Manages conflicts between bulk and streaming data sources
- **High Performance**: Processes 1000+ events per minute with minimal resource usage
- **Comprehensive Error Handling**: Circuit breaker pattern for API failures
- **Health Monitoring**: Built-in health checks and performance metrics
- **Structured Logging**: Detailed logging with contextual information

## Architecture

```
┌─────────────────────┐
│  Companies House    │
│  Streaming API      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  StreamingClient    │
│  - Authentication   │
│  - Connection Mgmt  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  EventProcessor     │
│  - Validation       │
│  - Data Extraction  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  DatabaseManager    │
│  - Upsert Logic     │
│  - Transactions     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    SQLite DB        │
│  - Companies        │
│  - Stream Events    │
└─────────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### 2. Set Environment Variables

```bash
export COMPANIES_HOUSE_API_KEY="your-api-key-here"
export STREAMING_API_URL="https://stream.companieshouse.gov.uk/companies"
```

### 3. Run Database Migrations

```bash
uv run python src/streaming/migrations.py
```

### 4. Start the Streaming Service

```bash
uv run python src/streaming/main.py
```

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `COMPANIES_HOUSE_API_KEY` | Your Companies House API key | - | Yes |
| `STREAMING_API_URL` | Streaming API endpoint | `https://stream.companieshouse.gov.uk/companies` | No |
| `DATABASE_PATH` | Path to SQLite database | `companies.db` | No |
| `LOG_LEVEL` | Logging level | `INFO` | No |
| `MAX_RETRIES` | Maximum reconnection attempts | `10` | No |
| `INITIAL_RETRY_DELAY` | Initial retry delay (seconds) | `1` | No |
| `MAX_RETRY_DELAY` | Maximum retry delay (seconds) | `300` | No |
| `CONNECTION_TIMEOUT` | Connection timeout (seconds) | `30` | No |
| `READ_TIMEOUT` | Read timeout (seconds) | `90` | No |
| `BUFFER_SIZE` | Event buffer size | `1000` | No |
| `BATCH_SIZE` | Database batch size | `100` | No |
| `HEALTH_CHECK_INTERVAL` | Health check interval (seconds) | `60` | No |
| `METRICS_ENABLED` | Enable performance metrics | `true` | No |
| `ERROR_ALERT_THRESHOLD` | Error threshold for alerts | `10` | No |
| `RATE_LIMIT_REQUESTS` | Requests per time window | `600` | No |
| `RATE_LIMIT_WINDOW` | Time window (seconds) | `300` | No |

### Configuration File (config.yaml)

```yaml
streaming:
  api:
    url: "https://stream.companieshouse.gov.uk/companies"
    timeout:
      connection: 30
      read: 90

  retry:
    max_attempts: 10
    initial_delay: 1
    max_delay: 300
    jitter: true

  processing:
    buffer_size: 1000
    batch_size: 100
    parallel_workers: 4

  monitoring:
    health_check_interval: 60
    metrics_enabled: true
    alert_threshold: 10

  logging:
    level: INFO
    format: json
    sample_rate: 0.1  # Log 10% of successful events
```

## API Usage

### Basic Usage

```python
from src.streaming import StreamingClient, EventProcessor, DatabaseManager

# Initialize components
client = StreamingClient(api_key="your-api-key")
processor = EventProcessor()
db_manager = DatabaseManager()

# Start streaming
async def stream_companies():
    async for event in client.stream():
        # Process event
        company_data = processor.process(event)

        # Update database
        await db_manager.upsert_company(company_data)
```

### With Error Handling

```python
from src.streaming import StreamingClient, CircuitBreaker

# Initialize with circuit breaker
circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60,
    expected_exception=ConnectionError
)

client = StreamingClient(
    api_key="your-api-key",
    circuit_breaker=circuit_breaker
)

# Stream with automatic retry and circuit breaking
async def resilient_stream():
    while True:
        try:
            async for event in client.stream():
                await process_event(event)
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            await asyncio.sleep(5)
```

### Health Monitoring

```python
from src.streaming import HealthMonitor

monitor = HealthMonitor(client)

# Get health status
health = await monitor.check_health()
print(f"Status: {health.status}")
print(f"Connected: {health.is_connected}")
print(f"Events/min: {health.events_per_minute}")
print(f"Last event: {health.last_event_time}")
```

## Database Schema

### Companies Table (Extended)

```sql
CREATE TABLE companies (
    company_number TEXT PRIMARY KEY,
    company_name TEXT,
    company_status TEXT,
    -- ... existing fields ...

    -- Streaming metadata
    stream_last_updated TEXT,
    stream_status TEXT DEFAULT 'unknown',
    data_source TEXT DEFAULT 'bulk',  -- 'bulk', 'stream', or 'both'
    last_stream_event_id TEXT,
    stream_metadata TEXT  -- JSON
);
```

### Stream Events Table

```sql
CREATE TABLE stream_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE,
    event_type TEXT NOT NULL,
    company_number TEXT,
    event_data TEXT,  -- JSON
    processed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (company_number) REFERENCES companies(company_number)
);
```

## Monitoring

### Metrics

The module collects the following metrics:

- **Connection Metrics**
  - Connection status (connected/disconnected)
  - Reconnection attempts
  - Connection duration

- **Processing Metrics**
  - Events received per minute
  - Events processed per minute
  - Processing latency (p50, p95, p99)
  - Error rate

- **Database Metrics**
  - Write operations per minute
  - Transaction success rate
  - Query performance

### Health Checks

Health checks are performed every 60 seconds and include:

1. **Connection Health**: Verifies API connection is active
2. **Processing Health**: Checks event processing pipeline
3. **Database Health**: Validates database connectivity
4. **Memory Health**: Monitors memory usage

### Logging

The module uses structured logging with the following levels:

- **DEBUG**: Detailed event processing information
- **INFO**: Normal operation events
- **WARNING**: Recoverable issues
- **ERROR**: Failures requiring attention
- **CRITICAL**: System failures

Example log entry:
```json
{
  "timestamp": "2025-01-15T10:30:45.123Z",
  "level": "INFO",
  "module": "streaming.client",
  "event": "event_processed",
  "company_number": "12345678",
  "event_type": "company_profile_changed",
  "processing_time_ms": 15,
  "correlation_id": "abc-123-def"
}
```

## Testing

### Run All Tests

```bash
# Run all streaming tests
uv run python -m pytest tests/streaming/ -v

# With coverage
uv run python -m pytest --cov=src/streaming tests/streaming/
```

### Test Categories

- **Unit Tests**: Test individual components
  ```bash
  uv run python -m pytest tests/streaming/test_event_processor.py
  ```

- **Integration Tests**: Test component interactions
  ```bash
  uv run python -m pytest tests/streaming/test_integration_pipeline.py
  ```

- **Load Tests**: Test performance under load
  ```bash
  uv run python -m pytest tests/streaming/test_load_testing.py
  ```

- **API Tests**: Test real API connectivity (requires valid API key)
  ```bash
  COMPANIES_HOUSE_API_KEY=your-key uv run python -m pytest tests/streaming/test_api_connectivity.py
  ```

## Troubleshooting

### Common Issues

#### 1. Connection Failures

**Symptom**: Unable to connect to streaming API
```
ERROR: Connection to streaming API failed: 401 Unauthorized
```

**Solutions**:
- Verify API key is correct
- Check API key has streaming permissions
- Ensure network connectivity
- Check firewall/proxy settings

#### 2. High Memory Usage

**Symptom**: Memory usage exceeds 100MB
```
WARNING: Memory usage high: 150MB
```

**Solutions**:
- Reduce `BUFFER_SIZE` configuration
- Increase `BATCH_SIZE` for database writes
- Enable log sampling to reduce logging overhead
- Check for memory leaks in custom processors

#### 3. Database Lock Errors

**Symptom**: Database is locked errors
```
ERROR: database is locked
```

**Solutions**:
- Ensure only one writer process is running
- Increase database timeout settings
- Use WAL mode for SQLite:
  ```sql
  PRAGMA journal_mode=WAL;
  ```

#### 4. Event Processing Delays

**Symptom**: Events taking >5 seconds to process
```
WARNING: Event processing delayed: 7.2s
```

**Solutions**:
- Check database performance
- Optimize database indexes
- Increase worker threads
- Review custom processing logic

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
export LOG_LEVEL=DEBUG
uv run python src/streaming/main.py
```

### Performance Tuning

For high-volume environments:

1. **Optimize Database**
   ```sql
   -- Enable WAL mode
   PRAGMA journal_mode=WAL;

   -- Increase cache size
   PRAGMA cache_size=10000;

   -- Optimize checkpointing
   PRAGMA wal_checkpoint(TRUNCATE);
   ```

2. **Adjust Buffer Sizes**
   ```bash
   export BUFFER_SIZE=5000
   export BATCH_SIZE=500
   ```

3. **Enable Connection Pooling**
   ```python
   db_manager = DatabaseManager(
       pool_size=10,
       max_overflow=20
   )
   ```

## Deployment

### Render Deployment (Recommended)

The streaming service is designed to run on Render as a Background Worker during UK business hours only, optimizing costs while maintaining real-time updates.

#### Quick Deploy

1. **Push to GitHub**:
   ```bash
   git add .
   git commit -m "Add streaming service"
   git push origin main
   ```

2. **Deploy to Render**:
   - Connect your GitHub repo to Render
   - Render will auto-detect `render.yaml` configuration
   - Set environment variable: `COMPANIES_HOUSE_API_KEY`
   - Service will automatically start/stop during UK business hours

#### Configuration

The service includes:
- **Background Worker**: $7/month for streaming service
- **Persistent Disk**: 10GB for SQLite database ($9/month)
- **Cron Jobs**: Free scheduling for business hours operation
- **Total Cost**: ~$16/month

#### Business Hours Operation

The service automatically:
- Starts at 8:30 AM UK time (Monday-Friday)
- Stops at 5:30 PM UK time
- Remains off during weekends and UK bank holidays
- Resumes from last processed event on restart

For detailed deployment instructions, see [docs/streaming/RENDER_DEPLOYMENT.md](../../docs/streaming/RENDER_DEPLOYMENT.md)

### Local Development

For local testing:
```bash
# Set environment variables
export COMPANIES_HOUSE_API_KEY="your-api-key"

# Run migrations
python src/streaming/migrations.py

# Start service
python src/streaming/main.py
```

## Maintenance

### Regular Tasks

- **Daily**: Check health metrics and error logs
- **Weekly**: Review performance metrics
- **Monthly**: Database optimization and cleanup
- **Quarterly**: Update dependencies and API changes

### Database Maintenance

```bash
# Optimize database
sqlite3 companies.db "VACUUM;"

# Clean old events (keep 90 days)
sqlite3 companies.db "DELETE FROM stream_events WHERE created_at < datetime('now', '-90 days');"

# Update statistics
sqlite3 companies.db "ANALYZE;"
```

## Support

For issues or questions:
1. Check the troubleshooting guide above
2. Review logs in `logs/streaming.log`
3. Check system health: `curl http://localhost:8080/health`
4. Contact support with correlation IDs from logs

## License

This module is part of the Companies House Scraper project.
