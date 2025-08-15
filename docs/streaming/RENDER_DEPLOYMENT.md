# Render Deployment Guide for Streaming Service

## Overview

This guide covers deploying the Companies House streaming service to Render as a Background Worker that runs during UK business hours only, optimizing costs while maintaining real-time data updates.

## Why Render?

- **Background Workers**: Perfect for long-running streaming connections ($7/month)
- **Persistent Disk**: SQLite database storage with automatic backups ($1/GB/month)
- **Business Hours Scheduling**: Built-in cron jobs to start/stop service
- **Auto-deploy**: GitHub integration for CI/CD
- **Zero DevOps**: No server management required

## Pricing Options

### Recommended: Pro Plan ($19/month)
The **Pro Plan** is the simplest and best value option:
- **$19 in usage credits** per month
- Covers all services with room to spare
- Credits roll over (up to $50)
- Priority support included
- No surprise overages

### Usage Breakdown on Pro Plan:
- **Background Worker (Starter)**: $7/month
- **Persistent Disk (10GB)**: $9/month (first 1GB free)
- **Grafana Monitoring**: Free (within credits)
- **Metrics Server**: Free (within credits)
- **Cron Jobs**: Free
- **Total Usage**: ~$16/month
- **Remaining Credits**: $3/month for growth

### Alternative: Pay-as-you-go
- Only pay for what you use
- Same services cost ~$16/month
- Risk of overages if usage spikes
- Community support only

## Prerequisites

1. **Render Account**: Sign up at [render.com](https://render.com)
2. **GitHub Repository**: Push your code to GitHub
3. **Companies House API Key**: Obtain from [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk)

## Deployment Steps

### 1. Prepare Your Repository

Ensure your repository has:
```
companies_house_scraper/
├── render.yaml                 # Render configuration
├── requirements.txt            # Python dependencies
├── src/
│   └── streaming/
│       ├── main.py            # Entry point
│       └── ...
└── deploy/
    └── render_scheduler.py    # Business hours scheduler
```

### 2. Connect GitHub to Render

1. Log into Render Dashboard
2. Click "New +" → "Background Worker"
3. Connect your GitHub account
4. Select your repository
5. Choose branch (usually `main`)

### 3. Configure Environment Variables

In Render Dashboard, add these environment variables:

```bash
# Required
COMPANIES_HOUSE_API_KEY=your-api-key-here

# Optional (defaults shown)
STREAMING_API_URL=https://stream.companieshouse.gov.uk/companies
DATABASE_PATH=/opt/render/project/data/companies.db
LOG_LEVEL=INFO
MAX_RETRIES=10
BUFFER_SIZE=1000
BATCH_SIZE=100
TZ=Europe/London
```

### 4. Set Up Persistent Disk

1. In your service settings, go to "Disks"
2. Add a new disk:
   - **Name**: `companies-data`
   - **Mount Path**: `/opt/render/project/data`
   - **Size**: 10 GB
3. Click "Save"

### 5. Deploy the Service

1. If using `render.yaml`:
   ```bash
   git add render.yaml
   git commit -m "Add Render configuration"
   git push origin main
   ```
   Render will auto-detect and use the configuration.

2. Or manually configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python src/streaming/main.py`

### 6. Set Up Business Hours Scheduling

#### Option A: Using Render Cron Jobs (Recommended)

1. Create two cron jobs in Render:

**Start Service (8:30 AM UK)**:
- **Schedule**: `30 8 * * 1-5`
- **Command**: `python deploy/render_scheduler.py start`
- **Environment Variables**:
  - `RENDER_API_KEY`: Your Render API key
  - `SERVICE_ID`: Your worker service ID

**Stop Service (5:30 PM UK)**:
- **Schedule**: `30 17 * * 1-5`
- **Command**: `python deploy/render_scheduler.py stop`
- **Environment Variables**: Same as above

#### Option B: Using External Scheduler

Use GitHub Actions or external cron service to call Render API:

```yaml
# .github/workflows/scheduler.yml
name: Business Hours Scheduler

on:
  schedule:
    - cron: '30 8 * * 1-5'   # Start at 8:30 AM UK
    - cron: '30 17 * * 1-5'  # Stop at 5:30 PM UK

jobs:
  control-service:
    runs-on: ubuntu-latest
    steps:
      - name: Control Render Service
        run: |
          if [[ "${{ github.event.schedule }}" == "30 8 * * 1-5" ]]; then
            ACTION="resume"
          else
            ACTION="suspend"
          fi

          curl -X POST \
            -H "Authorization: Bearer ${{ secrets.RENDER_API_KEY }}" \
            "https://api.render.com/v1/services/${{ secrets.SERVICE_ID }}/${ACTION}"
```

### 7. Monitor the Service

#### Grafana Dashboard (Included in Pro Plan)

The deployment includes a full Grafana monitoring stack:

1. **Access Grafana**:
   - URL: `https://companies-house-monitoring.onrender.com`
   - Login with credentials you set in environment variables

2. **Pre-configured Dashboard includes**:
   - Active strike-off companies count
   - Events processed per minute
   - Connection status indicator
   - Processing latency (p50, p95, p99)
   - Error rates by type
   - Database size tracking

3. **Metrics Server**:
   - URL: `https://companies-house-metrics.onrender.com/stats`
   - Prometheus endpoint: `/metrics`
   - Health check: `/health`

#### Native Render Monitoring

1. **Render Dashboard**: View logs, metrics, and status
2. **Health Endpoint**: Access `https://your-service.onrender.com/health`
3. **Alerts**: Set up in Render Dashboard under "Notifications"

## Database Management

### Initial Setup

```bash
# SSH into your service (Render Shell)
cd /opt/render/project/data

# Run migrations
python src/streaming/migrations.py

# Optimize database
sqlite3 companies.db << EOF
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=10000;
VACUUM;
ANALYZE;
EOF
```

### Backups

Render automatically backs up persistent disks daily. For additional backups:

```python
# backup_script.py (run as cron job)
import sqlite3
from datetime import datetime

source = '/opt/render/project/data/companies.db'
backup = f'/opt/render/project/data/backups/companies_{datetime.now():%Y%m%d}.db'

conn = sqlite3.connect(source)
backup_conn = sqlite3.connect(backup)
conn.backup(backup_conn)
backup_conn.close()
conn.close()
```

## Troubleshooting

### Service Won't Start

1. Check environment variables are set correctly
2. Verify API key has streaming permissions
3. Check logs in Render Dashboard
4. Ensure persistent disk is mounted

### High Memory Usage

```bash
# Add to environment variables
BUFFER_SIZE=500  # Reduce from 1000
BATCH_SIZE=50    # Reduce from 100
```

### Database Locked Errors

```bash
# Enable WAL mode
sqlite3 /opt/render/project/data/companies.db "PRAGMA journal_mode=WAL;"
```

### Missing Events During Restart

The service saves the last processed event ID. On restart, it resumes from that point. Check:
```bash
sqlite3 /opt/render/project/data/companies.db \
  "SELECT MAX(event_id) FROM stream_events;"
```

## Monitoring & Alerting

### Set Up Alerts in Render

1. Go to Settings → Notifications
2. Add alert for:
   - Service failures
   - High memory usage (>400MB)
   - Disk usage (>80%)

### Custom Health Checks

```python
# src/streaming/health.py
from fastapi import FastAPI
import sqlite3

app = FastAPI()

@app.get("/health")
async def health():
    try:
        # Check database
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute("SELECT 1")
        conn.close()

        # Check streaming connection
        # ... your checks ...

        return {"status": "healthy", "timestamp": datetime.now()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}, 503
```

## Security Best Practices

1. **Never commit API keys** - Use environment variables
2. **Restrict disk access** - Only your service should access the database
3. **Use Render's private services** - For internal communication
4. **Enable 2FA** - On both Render and GitHub accounts
5. **Audit logs** - Regularly review Render audit logs

## Scaling Considerations

### When to Scale

- Events/minute > 1000 consistently
- Memory usage > 450MB
- Processing latency > 5 seconds

### Scaling Options

1. **Vertical Scaling**: Upgrade to Standard plan ($25/month)
2. **Optimize Code**: Batch processing, better indexing
3. **Database Sharding**: Split by company number ranges

## Rollback Procedure

If deployment fails:

1. **Via Render Dashboard**:
   - Go to "Events" tab
   - Find previous successful deploy
   - Click "Rollback to this deploy"

2. **Via Git**:
   ```bash
   git revert HEAD
   git push origin main
   ```

## Support

- **Render Status**: [status.render.com](https://status.render.com)
- **Render Docs**: [render.com/docs](https://render.com/docs)
- **Community**: [community.render.com](https://community.render.com)

## Appendix: Render API Examples

### Get Service Status
```bash
curl -H "Authorization: Bearer $RENDER_API_KEY" \
  "https://api.render.com/v1/services/$SERVICE_ID"
```

### Suspend Service
```bash
curl -X POST \
  -H "Authorization: Bearer $RENDER_API_KEY" \
  "https://api.render.com/v1/services/$SERVICE_ID/suspend"
```

### Resume Service
```bash
curl -X POST \
  -H "Authorization: Bearer $RENDER_API_KEY" \
  "https://api.render.com/v1/services/$SERVICE_ID/resume"
```

### Get Logs
```bash
curl -H "Authorization: Bearer $RENDER_API_KEY" \
  "https://api.render.com/v1/services/$SERVICE_ID/logs"
```
