# Local Development Guide

This guide will get you up and running with local development in under 5 minutes using Docker, or with manual PostgreSQL setup if you prefer.

## Quick Start with Docker (Recommended)

**Prerequisites**: Docker and Docker Compose installed ([Get Docker](https://docs.docker.com/get-docker/))

### 1. Start PostgreSQL

```bash
docker-compose up -d
```

This starts PostgreSQL in the background with:
- **Database**: `companies_house`
- **User**: `dev`
- **Password**: `devpass`
- **Port**: `5432`

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and add your Companies House API keys (the Docker DATABASE_URL is already correct):

```bash
CH_STREAMING_API_KEY=your_streaming_key_here
CH_COMPANIES_API_KEY=your_companies_key_here
CH_OFFICERS_API_KEY=your_officers_key_here

# DATABASE_URL is already set for Docker
DATABASE_URL=postgresql://dev:devpass@localhost:5432/companies_house
```

### 3. Install Dependencies

```bash
uv sync
```

### 4. Run the Service

```bash
uv run python main.py
```

**That's it!** You're now running the streaming service locally.

---

## Docker Commands Reference

### Start PostgreSQL
```bash
docker-compose up -d
```

### Stop PostgreSQL
```bash
docker-compose down
```

### Reset Database (Wipe All Data)
```bash
docker-compose down -v
docker-compose up -d
```

### View PostgreSQL Logs
```bash
docker-compose logs -f postgres
```

### Connect to PostgreSQL CLI
```bash
docker-compose exec postgres psql -U dev -d companies_house
```

Useful SQL commands:
```sql
-- List all tables
\dt

-- Count companies
SELECT COUNT(*) FROM companies;

-- Count officers
SELECT COUNT(*) FROM officers;

-- View recent companies
SELECT company_number, company_name FROM companies ORDER BY id DESC LIMIT 10;

-- Exit
\q
```

---

## Manual PostgreSQL Setup (Alternative)

If you prefer not to use Docker, follow these steps:

### macOS

```bash
# Install PostgreSQL
brew install postgresql@15

# Start PostgreSQL service
brew services start postgresql@15

# Create database
createdb companies_house
```

### Ubuntu/Debian

```bash
# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql -c "CREATE DATABASE companies_house;"
sudo -u postgres psql -c "CREATE USER dev WITH PASSWORD 'devpass';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE companies_house TO dev;"
```

### Windows

1. Download PostgreSQL installer from [postgresql.org](https://www.postgresql.org/download/windows/)
2. Run installer and follow the wizard
3. Use pgAdmin or command line to create database:
   ```sql
   CREATE DATABASE companies_house;
   ```

### Configure .env for Manual Setup

Update your `.env` file with your local PostgreSQL credentials:

```bash
DATABASE_URL=postgresql://username:password@localhost:5432/companies_house
```

---

## Testing Your Setup

### Test Database Connection

```bash
uv run python scripts/test_database.py
```

Expected output:
```
Testing PostgreSQL connection...
✓ Connected to PostgreSQL successfully
Database: companies_house
✓ Can create tables
✓ Can insert data
✓ Can query data
All tests passed!
```

### Test Streaming API Connection

```bash
uv run python scripts/test_streaming_only.py
```

This will connect to the streaming API and show 5 live events to verify your API key works.

---

## Development Workflow

### 1. Make Code Changes

Edit files in `src/` directory using your preferred editor.

### 2. Run Code Quality Checks

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Type check
uv run mypy .
```

### 3. Run Tests

```bash
# All tests
uv run python -m pytest

# Specific module
uv run python -m pytest tests/streaming/ -v

# With coverage
uv run python -m pytest --cov=src tests/
```

### 4. Test Locally

```bash
# Run the service
uv run python main.py

# Watch logs for events being processed
```

Press `Ctrl+C` to stop the service.

### 5. Commit Changes

```bash
git add .
git commit -m "your commit message"
```

Pre-commit hooks will automatically run:
- Code formatting (ruff)
- Linting (ruff)
- Type checking (mypy)

If hooks fail, fix the issues and commit again.

---

## Debugging

### View Real-Time Event Processing

The service logs every event as it's processed:

```
2025-11-17 15:30:01 - Processing event for company 12345678
2025-11-17 15:30:02 - Synced company 12345678 to PostgreSQL
2025-11-17 15:30:03 - Synced 3 officers for 12345678
```

### Check Database Contents

Using Docker:
```bash
docker-compose exec postgres psql -U dev -d companies_house -c "SELECT COUNT(*) FROM companies;"
```

Or connect with any PostgreSQL client:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `companies_house`
- **Username**: `dev`
- **Password**: `devpass`

### Debug Streaming API Connection

Use the standalone streaming test:

```bash
uv run python scripts/test_streaming_only.py
```

This shows raw streaming events and helps debug API key issues.

### Common Issues

#### Port 5432 Already in Use

If you have PostgreSQL already running locally:

**Option 1**: Stop your local PostgreSQL
```bash
# macOS
brew services stop postgresql

# Ubuntu/Debian
sudo systemctl stop postgresql
```

**Option 2**: Change Docker port in `docker-compose.yml`:
```yaml
ports:
  - "5433:5432"  # Use port 5433 instead
```

Then update `.env`:
```bash
DATABASE_URL=postgresql://dev:devpass@localhost:5433/companies_house
```

#### Database Connection Refused

Make sure PostgreSQL is running:

```bash
# Docker
docker-compose ps

# Should show postgres as "Up"
```

#### API Rate Limiting

If you see 429 errors, you're hitting rate limits:
- Verify you're using 3 **different** API keys (not the same key 3 times)
- Check your rate limit status at [Companies House Developer Hub](https://developer.company-information.service.gov.uk/)

---

## Resetting Your Development Environment

### Wipe Database and Start Fresh

```bash
# With Docker
docker-compose down -v
docker-compose up -d

# Manual PostgreSQL
dropdb companies_house
createdb companies_house
```

### Reinstall Dependencies

```bash
rm uv.lock
uv sync
```

---

## Project Structure Overview

```
companies_house_scraper/
├── main.py          # Main entry point - start here
├── docker-compose.yml             # Local PostgreSQL setup
├── .env                          # Your API keys and config (not in git)
├── .env.example                  # Template for .env
│
├── src/                          # Source code
│   ├── streaming/               # Streaming API client
│   │   ├── client.py           # WebSocket connection handling
│   │   └── config.py           # API configuration
│   └── database/                # PostgreSQL integration
│       ├── client.py           # Connection pooling
│       ├── companies.py        # Company upsert logic
│       └── officers.py         # Officer upsert logic
│
├── tests/                       # Test suite
│   ├── streaming/              # Streaming API tests
│   └── database/               # Database tests
│
├── scripts/                     # Utility scripts
│   ├── test_database.py        # Test DB connection
│   ├── test_streaming_only.py  # Test API connection
│   └── test_workflow.py        # End-to-end workflow test
│
└── docs/                        # Documentation
    ├── LOCAL_DEVELOPMENT.md     # This file
    ├── database_README.md       # Database schema docs
    └── architecture/            # Architecture diagrams
```

### Key Files to Understand

1. **main.py** (200 lines)
   - Main service orchestration
   - Event queue management
   - Auto-reconnect logic
   - Rate limiting

2. **src/streaming/client.py** (350 lines)
   - Streaming API WebSocket connection
   - Event stream processing
   - Connection health monitoring

3. **src/database/companies.py** (100 lines)
   - Company upsert logic
   - PostgreSQL schema for companies table

4. **src/database/officers.py** (120 lines)
   - Officer upsert logic
   - PostgreSQL schema for officers table

---

## Making Your First Change

Here's a simple example to get familiar with the codebase:

### Example: Add a Log Message

1. Open `main.py`
2. Find the `_process_event` method (around line 182)
3. Add a log line:
   ```python
   logger.info(f"🎉 New event received for company {company_number}!")
   ```
4. Run the service:
   ```bash
   uv run python main.py
   ```
5. Watch for your emoji in the logs!

### Example: Add a Database Column

1. Open `src/database/companies.py`
2. Find the `CREATE TABLE` statement in `_create_tables()`
3. Add a column:
   ```sql
   custom_field TEXT
   ```
4. Reset database:
   ```bash
   docker-compose down -v
   docker-compose up -d
   ```
5. Run service - table will be recreated with new column

---

## Getting Help

- **Companies House API Docs**: https://developer.company-information.service.gov.uk/
- **PostgreSQL Docs**: https://www.postgresql.org/docs/
- **Python psycopg2 Docs**: https://www.psycopg.org/docs/
- **Railway Docs**: https://docs.railway.app/

---

## Next Steps

- Read `docs/database_README.md` for database schema details
- Review `docs/architecture/postgresql_streaming_architecture.svg` for system architecture
- Check `tests/` directory for test examples
- Explore `src/` to understand the codebase structure

Happy coding!
