# Companies House → PostgreSQL Integration

Real-time sync of Companies House data to PostgreSQL using the Streaming API.

## Overview

This system listens to the Companies House Streaming API for real-time company change events and automatically syncs company and officer data to PostgreSQL.

### Simplified Workflow

```
1. Streaming API → Company change events
2. Companies REST API → Fetch company data → PostgreSQL
3. Officers REST API → Fetch officer data → PostgreSQL
```

### Key Features

- **Real-time Sync**: Streaming API provides instant notifications of company changes
- **Separate Rate Limits**: 3 independent CH Developer Hub applications = 1800 requests per 5 minutes total
- **PostgreSQL Database**: Production-ready database with concurrent access support
- **n8n Compatible**: Native PostgreSQL support for workflow automation
- **Automatic Upsert**: Creates new records or updates existing ones automatically
- **Railway Ready**: Deploy to Railway with zero configuration

## Architecture

### 3 Companies House Developer Hub Applications

This integration uses **3 separate** CH Developer Hub applications for independent rate limiting:

| Application | Purpose | Rate Limit | Environment Variable |
|-------------|---------|------------|---------------------|
| **App #1** | Streaming API | No limit | `CH_STREAMING_API_KEY` |
| **App #2** | Companies REST API | 600 req / 5 min | `CH_COMPANIES_API_KEY` |
| **App #3** | Officers REST API | 600 req / 5 min | `CH_OFFICERS_API_KEY` |

**Total capacity**: 1800 API requests per 5 minutes (Companies + Officers combined)

### Data Flow Diagram

See: `docs/architecture/simplified_streaming_architecture.svg`

## Setup

### Prerequisites

- Python 3.9+
- PostgreSQL database (local or Railway)
- 3 Companies House Developer Hub applications (see above)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/companies_house_scraper.git
cd companies_house_scraper
```

2. Install dependencies using `uv`:
```bash
uv sync
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database URL and API keys
```

### PostgreSQL Setup

#### Option 1: Railway (Recommended)

See `docs/RAILWAY_DEPLOYMENT.md` for complete Railway deployment guide with PostgreSQL.

Railway provides:
- Free PostgreSQL database
- Automatic DATABASE_URL configuration
- Zero-configuration deployment
- n8n compatibility

#### Option 2: Local PostgreSQL

1. Install PostgreSQL locally
2. Create a database:
```bash
createdb companies_house
```

3. Add connection URL to `.env`:
```bash
DATABASE_URL=postgresql://user:password@localhost:5432/companies_house
```

4. Test database connection:
```bash
uv run python test_database.py
```

The schema will be automatically initialized on first run.

### Companies House API Setup

1. Create 3 separate applications at: https://developer.company-information.service.gov.uk/
   - **App #1**: "Streaming API Access"
   - **App #2**: "Companies REST API Access"
   - **App #3**: "Officers REST API Access"

2. Add the API keys to `.env`:
```bash
CH_STREAMING_API_KEY=your_streaming_key
CH_COMPANIES_API_KEY=your_companies_key
CH_OFFICERS_API_KEY=your_officers_key
```

## Running the Service

Start the streaming service:

```bash
uv run python streaming_service.py
```

The service will:
1. Initialize PostgreSQL database schema
2. Connect to Companies House Streaming API
3. Listen for company change events
4. For each event:
   - Fetch company data from CH Companies API
   - Upsert company to PostgreSQL
   - Fetch officers data from CH Officers API
   - Upsert officers to PostgreSQL

### Monitoring

The service logs:
- Events received
- Companies synced
- Officers synced
- Errors encountered

Example output:
```
2025-11-17 15:30:00 - Starting simplified streaming service...
2025-11-17 15:30:00 - Initializing database schema...
2025-11-17 15:30:01 - Database schema initialized successfully
2025-11-17 15:30:01 - Processing event for company 12345678
2025-11-17 15:30:02 - Synced company 12345678 to PostgreSQL
2025-11-17 15:30:03 - Synced 3 officers for 12345678
```

## Project Structure

```
companies_house_scraper/
├── streaming_service.py          # Main service entry point
├── test_database.py              # Database testing script
├── src/
│   ├── database/                # PostgreSQL integration
│   │   ├── client.py           # Connection pooling and schema
│   │   ├── companies.py        # Company upsert operations
│   │   └── officers.py         # Officer upsert operations
│   ├── streaming/               # Streaming API client
│   └── monitoring/              # Metrics and monitoring
├── docs/
│   ├── RAILWAY_DEPLOYMENT.md    # Railway deployment guide
│   ├── database_README.md       # Database schema documentation
│   └── architecture/            # Architecture diagrams
├── tests/                       # Test suite
├── .env.example                 # Environment template
├── pyproject.toml               # Dependencies and config
└── archive/                     # Archived old workflow files
```

## Development

### Code Quality

This project follows strict code quality standards:

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Type check
uv run mypy .

# Run tests
uv run python -m pytest tests/ -v
```

All checks must pass before committing.

### Testing

Run the test suite:

```bash
# All tests
uv run python -m pytest

# Specific module
uv run python -m pytest tests/streaming/

# With coverage
uv run python -m pytest --cov=src tests/
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CH_STREAMING_API_KEY` | Yes | Streaming API key (App #1) |
| `CH_COMPANIES_API_KEY` | Yes | Companies REST API key (App #2) |
| `CH_OFFICERS_API_KEY` | Yes | Officers REST API key (App #3) |
| `DATABASE_URL` | Yes | PostgreSQL connection URL |
| `LOG_LEVEL` | No | Logging level (default: "INFO") |

**DATABASE_URL Format**:
```
postgresql://username:password@host:port/database
```

On Railway, this is automatically provided when you add PostgreSQL.

## Architecture Decisions

### Why 3 Separate CH Applications?

Each Companies House application has a 600 requests / 5 minutes rate limit. By using 3 separate applications:
- **Streaming API** (App #1): No rate limit, continuous stream
- **Companies API** (App #2): 600 req/5min dedicated to company data
- **Officers API** (App #3): 600 req/5min dedicated to officer data
- **Total**: 1800 API requests per 5 minutes

This prevents one API from blocking the other and maximizes throughput.

### Why PostgreSQL?

- **Production-Ready**: Battle-tested database with ACID compliance
- **Concurrent Access**: Multiple services can read/write simultaneously
- **n8n Integration**: Native PostgreSQL support for workflow automation
- **Railway Support**: Zero-configuration deployment with automatic DATABASE_URL
- **Persistent Storage**: Data survives container restarts
- **Standard SQL**: Universal query language for data access
- **Free Tier**: Railway provides PostgreSQL in hobby plan

### Why In-Memory Queue?

The simplified workflow doesn't need persistent queue storage because:
- **Fast Processing**: Events are processed immediately
- **PostgreSQL as Source of Truth**: All data is in PostgreSQL, not local queue
- **Restart Capability**: Streaming API will resend events on reconnection
- **Reduced Complexity**: No queue management or message brokers needed

## Deployment

### Railway Deployment (Recommended)

See `docs/RAILWAY_DEPLOYMENT.md` for complete deployment guide.

Quick steps:
1. Push code to GitHub
2. Create Railway project from GitHub repo
3. Add PostgreSQL database in Railway
4. Set environment variables (CH API keys)
5. Deploy - DATABASE_URL is automatic!

### Manual Deployment

For production deployment, consider:

1. **Process Manager**: Use systemd, supervisor, or Docker to keep service running
2. **PostgreSQL**: Set up managed PostgreSQL (Railway, AWS RDS, DigitalOcean)
3. **Monitoring**: Set up alerts for service downtime or error rates
4. **Logging**: Configure log aggregation (e.g., CloudWatch, Datadog)
5. **Secrets Management**: Use environment variable management (e.g., AWS Secrets Manager)

## Troubleshooting

### Service Won't Start

- Verify all environment variables are set in `.env`
- Check API keys are valid at Companies House Developer Hub
- Verify DATABASE_URL is correct and database is accessible
- Test database connection with `uv run python test_database.py`

### Rate Limiting Errors

- Confirm you're using 3 separate CH applications (not the same key 3 times)
- Check rate limit status in CH Developer Hub dashboard
- Reduce event processing rate if needed

### Database Connection Errors

- Verify DATABASE_URL format: `postgresql://user:password@host:port/database`
- Check database server is running and accessible
- Ensure user has CREATE TABLE permissions for schema initialization
- For Railway: verify PostgreSQL database is added to project

## Contributing

1. Follow code quality standards (see Development section)
2. Write tests for new features
3. Update documentation as needed
4. Run all checks before committing

## License

MIT License - See LICENSE file for details

## Links

- [Companies House Developer Hub](https://developer.company-information.service.gov.uk/)
- [Companies House Streaming API Docs](https://developer-specs.company-information.service.gov.uk/streaming-api/reference)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Railway Documentation](https://docs.railway.app/)
- [n8n PostgreSQL Integration](https://docs.n8n.io/integrations/builtin/app-nodes/n8n-nodes-base.postgres/)
- [Architecture Diagram](docs/architecture/simplified_streaming_architecture.svg)
- [Railway Deployment Guide](docs/RAILWAY_DEPLOYMENT.md)
- [Database Schema](docs/database_README.md)
