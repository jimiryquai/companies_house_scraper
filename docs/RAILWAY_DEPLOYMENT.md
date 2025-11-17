# Railway Deployment Guide

This guide explains how to deploy the Companies House Streaming Service to Railway with PostgreSQL.

## Why Railway + PostgreSQL?

- **Managed PostgreSQL**: Railway provides free PostgreSQL databases with automatic backups
- **n8n Integration**: n8n has native PostgreSQL support for data access
- **Concurrent Access**: PostgreSQL handles multiple connections (streaming service + n8n)
- **Persistent Storage**: Unlike SQLite, data persists across container restarts
- **Zero Configuration**: DATABASE_URL is automatically injected by Railway

## Prerequisites

1. Railway account ([railway.app](https://railway.app))
2. GitHub repository with your code
3. Companies House Developer Hub API keys (3 separate applications)

## Deployment Steps

### 1. Create New Railway Project

1. Visit [railway.app](https://railway.app) and sign in
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"**
4. Choose your `companies_house_scraper` repository
5. Railway will automatically detect the Python project

### 2. Add PostgreSQL Database

1. In your Railway project, click **"New"** → **"Database"** → **"Add PostgreSQL"**
2. Railway automatically creates a PostgreSQL database
3. The `DATABASE_URL` environment variable is automatically set and shared with your service
4. No configuration needed - the connection is automatic

### 3. Configure Environment Variables

In your Railway service settings, add these environment variables:

```bash
# Companies House API Keys (from Developer Hub)
CH_STREAMING_API_KEY=your_streaming_api_key_here
CH_COMPANIES_API_KEY=your_companies_rest_api_key_here
CH_OFFICERS_API_KEY=your_officers_rest_api_key_here

# Logging (optional)
LOG_LEVEL=INFO
```

**Note**: You do NOT need to set `DATABASE_URL` - Railway sets this automatically when you add PostgreSQL.

### 4. Configure Start Command

Railway should automatically detect the start command, but if needed, set:

```bash
uv run python streaming_service.py
```

### 5. Deploy

1. Click **"Deploy"** in Railway
2. Railway will:
   - Install dependencies using `uv`
   - Connect to PostgreSQL automatically
   - Initialize database schema on first run
   - Start the streaming service

### 6. Monitor Deployment

- View logs in Railway dashboard
- Check for successful database connection
- Verify schema initialization
- Monitor event processing

## Database Access for n8n

Once deployed, you can access the PostgreSQL database from n8n using Railway's connection details.

### Get Connection Details

In Railway, click on your PostgreSQL database and copy:
- **Host**: `<region>.railway.app`
- **Port**: `5432` (or custom port shown)
- **Database**: (shown in Railway)
- **Username**: (shown in Railway)
- **Password**: (shown in Railway)

Or use the full `DATABASE_URL` connection string directly.

### Configure n8n PostgreSQL Node

1. Add a **PostgreSQL** node in n8n
2. Create new credentials with Railway's connection details:
   - Host: From Railway
   - Port: From Railway (usually 5432)
   - Database: From Railway
   - User: From Railway
   - Password: From Railway
   - SSL: Enabled (Railway requires SSL)

3. Test connection - you should see the `companies` and `officers` tables

### Example n8n Queries

**Get recent companies:**
```sql
SELECT * FROM companies
ORDER BY created_at DESC
LIMIT 10;
```

**Get companies with officers:**
```sql
SELECT
  c.company_number,
  c.company_name,
  c.company_status,
  COUNT(o.unique_key) as officer_count
FROM companies c
LEFT JOIN officers o ON c.company_number = o.company_number
GROUP BY c.company_number, c.company_name, c.company_status
ORDER BY c.created_at DESC;
```

**Get specific company officers:**
```sql
SELECT
  o.name,
  o.officer_role,
  o.appointed_on,
  o.nationality
FROM officers o
WHERE o.company_number = '12345678'
ORDER BY o.appointed_on DESC;
```

## Database Schema

### Companies Table

| Column | Type | Description |
|--------|------|-------------|
| company_number | VARCHAR(20) | Primary key |
| company_name | TEXT | Company name |
| company_status | VARCHAR(50) | active, dissolved, etc. |
| company_type | VARCHAR(50) | ltd, plc, etc. |
| jurisdiction | VARCHAR(50) | england-wales, scotland, etc. |
| date_of_creation | DATE | Incorporation date |
| date_of_cessation | DATE | Dissolution date |
| registered_office_address | TEXT | Full address |
| postal_code | VARCHAR(20) | Postcode |
| locality | VARCHAR(100) | City/town |
| sic_codes | TEXT | SIC codes (comma-separated) |
| ch_url | TEXT | Companies House URL |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |

### Officers Table

| Column | Type | Description |
|--------|------|-------------|
| unique_key | VARCHAR(100) | Primary key |
| company_number | VARCHAR(20) | Foreign key to companies |
| name | TEXT | Officer name |
| officer_role | VARCHAR(50) | director, secretary, etc. |
| appointed_on | DATE | Appointment date |
| resigned_on | DATE | Resignation date |
| nationality | VARCHAR(50) | Officer nationality |
| occupation | VARCHAR(100) | Occupation |
| country_of_residence | VARCHAR(50) | Country of residence |
| date_of_birth | VARCHAR(10) | Partial DOB (YYYY-MM) |
| address | TEXT | Full address |
| postal_code | VARCHAR(20) | Postcode |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |

## Service Architecture

The deployed service uses 3 separate Companies House Developer Hub applications:

1. **Streaming API App** - Listens for real-time company events
2. **Companies REST API App** - Fetches company data (600 req/5min)
3. **Officers REST API App** - Fetches officer data (600 req/5min)

This architecture provides **1800 total requests per 5 minutes** (3 × 600).

## Monitoring and Logs

### View Logs in Railway

1. Click on your service in Railway
2. View **"Logs"** tab for real-time output
3. Look for:
   - Database connection success
   - Schema initialization
   - Event processing
   - Upsert operations

### Expected Log Output

```
INFO - Starting simplified streaming service...
INFO - Initializing database schema...
INFO - Database schema initialized successfully
INFO - Starting to stream events...
INFO - Starting event processor...
INFO - Processing event for company 12345678
INFO - Synced company 12345678 to PostgreSQL
INFO - Synced 3 officers for 12345678
```

## Troubleshooting

### Database Connection Failed

**Error**: `could not connect to server`

**Solution**:
- Verify PostgreSQL database is running in Railway
- Check that DATABASE_URL is set (automatic in Railway)
- Ensure service and database are in the same project

### Schema Initialization Failed

**Error**: `relation "companies" already exists`

**Solution**: This is normal on restarts - the schema already exists and will be reused.

### API Rate Limiting

**Error**: `429 Too Many Requests`

**Solution**:
- Verify you're using 3 separate API keys
- Check Companies House Developer Hub rate limits
- Ensure keys are correctly set in environment variables

### n8n Cannot Connect

**Error**: `connection refused` or `SSL required`

**Solution**:
- Enable SSL in n8n PostgreSQL credentials
- Use connection details from Railway PostgreSQL database
- Ensure Railway database is publicly accessible (default)

## Production Checklist

- [ ] PostgreSQL database created in Railway
- [ ] All 3 API keys configured in environment variables
- [ ] Service deployed and running
- [ ] Database schema initialized successfully
- [ ] Event processing confirmed in logs
- [ ] n8n can connect to PostgreSQL
- [ ] Sample queries work in n8n
- [ ] Monitoring/alerting configured (optional)

## Cost Estimate

Railway pricing (as of 2024):
- **Hobby Plan**: $5/month includes:
  - 512MB RAM
  - PostgreSQL database
  - Sufficient for this use case
- **Free Trial**: $5 credit to test deployment

## Next Steps

1. Deploy to Railway following this guide
2. Verify streaming service is processing events
3. Configure n8n to access the PostgreSQL database
4. Create n8n workflows using the company and officer data
5. Monitor performance and adjust as needed

## Support

For issues:
- Railway docs: [docs.railway.app](https://docs.railway.app)
- Companies House API: [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk)
- PostgreSQL docs: [postgresql.org/docs](https://www.postgresql.org/docs/)
