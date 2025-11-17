# Quick Deployment Instructions

## ✅ Local Testing Complete

Your system is working perfectly:
- ✅ Database connection and schema
- ✅ Companies House API integration
- ✅ Company data fetch and storage
- ✅ Officers data fetch and storage
- ✅ Data verified in PostgreSQL

**Test Results**: Successfully processed company `00000006` with 34 officers!

---

## Railway Deployment Options

### Option 1: Railway CLI (Fastest - 2 minutes)

1. **Login to Railway**:
```bash
railway login
```
This will open your browser for authentication.

2. **Run deployment script**:
```bash
./deploy_to_railway.sh
```

The script will:
- Create a new Railway project
- Add PostgreSQL database
- Set your API keys as environment variables
- Deploy the service

3. **Monitor deployment**:
```bash
railway logs
```

4. **Get DATABASE_URL for n8n**:
```bash
railway variables
```

---

### Option 2: Railway Web UI (Easier - 5 minutes)

1. **Go to [railway.app](https://railway.app)** and sign in

2. **Create New Project**:
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your `companies_house_scraper` repository
   - Railway will automatically detect Python project

3. **Add PostgreSQL**:
   - In your project, click "New" → "Database" → "Add PostgreSQL"
   - DATABASE_URL is automatically set

4. **Set Environment Variables**:
   - Click on your service
   - Go to "Variables" tab
   - Add these variables:
     ```
     CH_STREAMING_API_KEY=d0b7e04e-a90f-4a7b-b48b-83a0b0bf8eed
     CH_COMPANIES_API_KEY=fdb5bed5-a7f6-4547-8157-86b7e49c6095
     CH_OFFICERS_API_KEY=<your_third_api_key>
     LOG_LEVEL=INFO
     ```

5. **Deploy**:
   - Click "Deploy"
   - Railway will build and start your service

6. **Verify**:
   - Click "Logs" to see the service running
   - You should see:
     ```
     Starting simplified streaming service...
     Initializing database schema...
     Database schema initialized successfully
     Starting to stream events...
     ```

7. **Get PostgreSQL Connection for n8n**:
   - Click on the PostgreSQL database
   - Copy connection details or full DATABASE_URL
   - Use this in n8n to access your data

---

## Important Notes

### API Keys

You currently have 2 Companies House API keys set up:
- `CH_STREAMING_API_KEY`: d0b7e04e-a90f-4a7b-b48b-83a0b0bf8eed
- `CH_COMPANIES_API_KEY`: fdb5bed5-a7f6-4547-8157-86b7e49c6095

**You need to create a 3rd API key** for `CH_OFFICERS_API_KEY`:
1. Go to [Companies House Developer Hub](https://developer.company-information.service.gov.uk/)
2. Create a new application called "Officers REST API"
3. Copy the API key and use it for `CH_OFFICERS_API_KEY`

This gives you 1800 total requests per 5 minutes (600 per app × 3 apps).

### After Deployment

Once deployed, the service will:
1. Automatically initialize the database schema
2. Connect to Companies House Streaming API
3. Start processing company change events
4. Store company and officer data in PostgreSQL

### Accessing Data with n8n

Your client's developer can access the data using n8n:

1. **Get Connection Details** from Railway:
   - Host: `<region>.railway.app`
   - Port: `5432`
   - Database, Username, Password: shown in Railway
   - Or use full DATABASE_URL string

2. **Configure n8n PostgreSQL Node**:
   - Add PostgreSQL credentials in n8n
   - Enable SSL (required by Railway)
   - Test connection

3. **Example Queries** (see `docs/RAILWAY_DEPLOYMENT.md` for more):
```sql
-- Get recent companies
SELECT * FROM companies ORDER BY created_at DESC LIMIT 10;

-- Get company with officers
SELECT
  c.company_name,
  COUNT(o.unique_key) as officer_count
FROM companies c
LEFT JOIN officers o ON c.company_number = o.company_number
GROUP BY c.company_number, c.company_name
ORDER BY c.created_at DESC;
```

---

## Troubleshooting

### Deployment Fails
- Check all environment variables are set
- Verify API keys are correct
- Check Railway logs for errors

### Database Connection Errors
- Ensure PostgreSQL database is added to project
- DATABASE_URL is automatically set by Railway
- Check service and database are in same project

### No Events Being Processed
- Verify CH_STREAMING_API_KEY is correct
- Check service logs for connection errors
- Streaming events may be infrequent initially

---

## Next Steps

1. **Deploy** using either Option 1 or Option 2 above
2. **Monitor logs** to ensure events are being processed
3. **Share DATABASE_URL** with your client's developer for n8n access
4. **Hand off** the project - you're done!

---

## Quick Reference

```bash
# Railway CLI Commands
railway login          # Login to Railway
railway init           # Create new project
railway add            # Add PostgreSQL
railway variables      # View/set environment variables
railway up             # Deploy
railway logs           # View logs
railway open           # Open in browser
railway status         # Check deployment status
```

Good luck with the deployment! 🚀
