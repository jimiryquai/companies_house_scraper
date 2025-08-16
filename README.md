# Companies House Strike-Off Lead Generator

A Python-based system for identifying UK companies in distress (with "Active - Proposal to Strike Off" status) and extracting director information for Business Reset Group's lead generation efforts.

## Overview

This system processes Companies House bulk data to identify companies facing potential closure and enriches this data with officer/director information via the Companies House API. The data is stored in a local SQLite database and can be exported to CSV for CRM integration.

## Current Workflow (Two-Step Process)

### Step 1: Import Companies from Bulk Data
1. Download the latest bulk company data CSV from Companies House:
   - Visit: https://download.companieshouse.gov.uk/en_output.html
   - Download: `BasicCompanyDataAsOneFile-YYYY-MM-DD.csv`
   - Place the CSV file in the project root directory

2. Run the import script to filter and store companies with strike-off status:
   ```bash
   python import_companies.py
   ```
   This will:
   - Parse the CSV file (contains millions of records)
   - Filter for companies with status "Active - Proposal to Strike Off"
   - Store filtered companies in `companies.db` SQLite database
   - Process in batches for memory efficiency

### Step 2: Fetch Officer/Director Information
3. Run the officer import script to get director details via API:
   ```bash
   python officer_import.py
   ```
   This will:
   - Read companies with strike-off status from the database
   - Make API calls to get officer/director information for each company
   - Respect Companies House API rate limits (600 calls per 5 minutes)
   - Save officer data to the database
   - Support resumable processing (can be interrupted and restarted)
   - **Note:** This process takes several days due to API rate limits

### Step 3: Export Data
4. Export the enriched data to CSV:
   ```bash
   python export_to_csv.py
   ```
   This creates CSV files with company and officer information ready for CRM import.

## Project Structure

```
companies_house_scraper/
├── import_companies.py      # Step 1: Import companies from bulk CSV
├── officer_import.py        # Step 2: Fetch officers via API
├── export_to_csv.py        # Step 3: Export to CSV
├── config.yaml             # Configuration (API keys, settings)
├── companies.db            # SQLite database
├── .agent-os/              # Agent OS documentation
│   └── product/           # Product specifications
└── archive/               # Experimental/unused scripts
    ├── import_experiments/ # Various import script iterations
    ├── api_experiments/   # API-based approaches
    └── logs/             # Historical log files
```

## Configuration

Edit `config.yaml` to set your Companies House API key:

```yaml
api:
  key: YOUR_API_KEY_HERE

rate_limit:
  calls: 600
  period: 300  # 5 minutes
```

Get your API key from: https://developer.company-information.service.gov.uk/

## Database Schema

### companies table
- `company_number` - Unique company identifier
- `company_name` - Company name
- `company_status` - Current status
- `company_status_detail` - Detailed status (e.g., "Active - Proposal to Strike Off")
- `incorporation_date` - Date of incorporation
- `sic_codes` - Standard Industrial Classification codes
- Various address fields

### officers table
- `company_number` - Links to companies table
- `name` - Officer name
- `officer_role` - Role (director, managing-director, etc.)
- `appointed_on` - Appointment date
- `resigned_on` - Resignation date (if applicable)
- Various address and personal detail fields

## Requirements

- Python 3.8+
- Dependencies (install with `pip install -r requirements.txt`):
  - requests
  - pyyaml
  - loguru
  - pydantic
  - python-dateutil
  - tqdm

## Processing Timeline

Based on May 2025 implementation:
- **CSV Import**: ~30 minutes for full UK company dataset
- **Officer Fetching**: 3-5 days (due to API rate limits)
- **Total Companies with Strike-Off Status**: ~15,000-20,000
- **Directors per Company**: Average 2-3

## Future Enhancements (Planned)

1. **Real-time Updates**: Integration with Companies House Streaming API
2. **Data Enrichment**: Apollo.io integration for contact details
3. **CRM Integration**: Direct sync with GoHighLevel
4. **Cloud Deployment**: Migration to Cloudflare Workers + D1 database
5. **Web Interface**: Dashboard for monitoring and management

## Historical Context

This project was initially developed in May 2025 as a one-time data extraction tool. Due to its success in generating quality leads for Business Reset Group, it's being evolved into an automated, recurring process with real-time monitoring capabilities.

## Support

For issues or questions about this system, please contact the development team or refer to the Agent OS documentation in `.agent-os/product/`.
