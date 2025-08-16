# Product Mission

## Pitch

Companies House Scraper is a lead generation system that helps Business Reset Group identify UK companies in distress by automatically extracting and enriching data from Companies House records to generate qualified leads for distressed company assistance services.

## Users

### Primary Customers

- **Business Reset Group**: Company providing assistance to distressed company directors, using this tool for lead generation
- **Business development teams**: Sales and marketing professionals who need qualified leads from distressed companies

### User Personas

**Business Development Manager** (30-45 years old)
- **Role:** Lead Generation Specialist
- **Context:** Works at Business Reset Group identifying potential clients who need assistance with company restructuring or closure
- **Pain Points:** Manual research is time-consuming, outdated data sources, difficulty identifying companies at the right stage of distress
- **Goals:** Generate high-quality leads efficiently, identify companies before competitors, maintain accurate prospect database

## The Problem

### Manual Lead Generation is Inefficient

Business Reset Group currently relies on manual research to identify distressed companies, which is time-consuming and often misses opportunities. This results in reduced lead volume and competitors reaching prospects first.

**Our Solution:** Automated identification and data extraction from Companies House records with real-time updates.

### Outdated Company Information

Static data sources become quickly outdated, leading to wasted effort contacting dissolved companies or missing newly distressed businesses. This impacts conversion rates and resource allocation.

**Our Solution:** Integration with Companies House Streaming API for real-time status updates and bulk data processing.

### Incomplete Prospect Data

Basic company information lacks the enriched data needed for effective outreach, requiring additional manual research before contact. This delays the sales process and reduces team productivity.

**Our Solution:** Automated data enrichment through Apollo.io integration and comprehensive director information extraction.

## Differentiators

### Real-Time Distress Signal Detection

Unlike generic company databases, we specifically monitor "Active - Proposal to Strike Off" status changes in real-time through Companies House Streaming API. This results in first-mover advantage on fresh leads.

### Comprehensive Director Intelligence

Unlike basic company search tools, we automatically extract complete director information including personal details and company history. This provides immediate context for personalized outreach and relationship mapping.

### Purpose-Built for Distressed Companies

Unlike general business intelligence tools, we focus exclusively on companies showing distress signals with specialized filtering and enrichment. This results in higher-quality leads with better conversion potential.

## Key Features

### Core Features

- **Companies House API Integration:** Real-time access to official UK company data with automatic status monitoring
- **Distress Signal Filtering:** Automated identification of companies with "Active - Proposal to Strike Off" status
- **Director Information Extraction:** Complete officer details including names, addresses, and appointment history
- **SQLite Database Storage:** Local data persistence with efficient querying and relationship tracking
- **Bulk Data Processing:** Import and process large Companies House data snapshots for comprehensive coverage
- **CSV Export Functionality:** Export filtered results for CRM import and team distribution

### Automation Features

- **Real-Time Status Updates:** Streaming API integration for immediate notification of status changes
- **Automated Data Enrichment:** Apollo.io integration for enhanced contact information and company insights
- **Progress Tracking:** Resumable processing with detailed progress monitoring and error recovery
- **Retry Mechanisms:** Robust error handling for reliable data collection despite API limitations

### Integration Features

- **CRM Integration:** Direct export to GoHighLevel for seamless sales workflow integration
- **Cloud Deployment:** Scalable processing using Cloudflare Workers and D1 database
