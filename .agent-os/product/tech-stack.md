# Technical Stack

## Application Framework
**Python 3.8+**
- Core runtime environment for the scraper application
- Chosen for excellent API integration capabilities and data processing libraries

## Database System
**SQLite**
- Local database storage for company and officer data
- Lightweight, serverless database perfect for local development and data persistence
- Future migration path to Cloudflare D1 for cloud deployment

## JavaScript Framework
**N/A**
- Pure Python application with no frontend JavaScript requirements
- Future web interface may use vanilla JavaScript or lightweight framework

## Import Strategy
**N/A**
- Python package management via pip and pyproject.toml
- Dependencies managed through standard Python packaging tools

## CSS Framework
**N/A**
- No current UI requirements
- Future web interface will use modern CSS framework (TBD)

## UI Component Library
**N/A**
- Command-line interface application
- Future web interface will require component library selection

## Fonts Provider
**N/A**
- No current UI requirements

## Icon Library
**N/A**
- No current UI requirements

## Application Hosting
**Local Development / Cloudflare Workers (Planned)**
- Currently runs locally for development and testing
- Planned deployment to Cloudflare Workers for serverless execution
- Consideration for other cloud providers based on requirements

## Database Hosting
**Local SQLite / Cloudflare D1 (Planned)**
- Current: Local SQLite database files
- Planned: Migration to Cloudflare D1 for cloud-native database

## Asset Hosting
**N/A**
- No static assets currently required
- Future requirements will use cloud storage solution

## Deployment Solution
**Manual Deployment / GitHub Actions (Planned)**
- Current: Manual execution in development environment
- Planned: Automated deployment pipeline with GitHub Actions
- Target: Cloudflare Workers deployment automation

## Code Repository
**Local Git Repository**
- Currently not connected to remote repository
- Future: GitHub repository for version control and collaboration

## Core Dependencies

### Data Processing
- **csv**: Built-in Python CSV processing for bulk data import
- **requests**: HTTP client for Companies House API integration
- **pydantic**: Data validation and settings management
- **pyyaml**: Configuration file management

### Logging and Monitoring
- **loguru**: Advanced logging capabilities with structured output
- **tqdm**: Progress bars for long-running operations
- **pytest**: Testing framework for unit and integration tests

### Database
- **sqlite3**: Built-in Python SQLite interface for local data storage
- **sqlalchemy** (future): ORM for complex database operations when migrating to cloud

### External Data Sources
- **Companies House Bulk Data**: CSV snapshots (BasicCompanyDataAsOneFile)
- **Companies House API**: Officer/director information retrieval
- **Apollo.io API** (planned): Contact enrichment service
- **GoHighLevel API** (planned): CRM integration
- **Companies House Streaming API** (planned): Real-time status updates

## Architecture Patterns

### Data Processing Pipeline
- ETL pattern for bulk data import and processing
- Async processing for API rate limit management
- Retry mechanisms with exponential backoff

### Configuration Management
- YAML-based configuration files
- Environment-specific configuration support
- Secure credential management

### Testing Strategy
- Unit tests with pytest
- Integration tests for API endpoints
- Data validation testing for imports