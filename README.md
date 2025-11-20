# BVP Portfolio Jobs Intelligence

Automated scraper and analytics system for tracking job openings across Bessemer Venture Partners' 350+ portfolio companies.

## Overview

This system:
- Scrapes 5,800+ job listings from jobs.bvp.com weekly
- Analyzes jobs by function, level, location, and remote work status
- Loads data into Airtable for easy access and visualization
- Tracks hiring trends over time via weekly snapshots
- Identifies talent pooling opportunities across multiple companies

## Setup

### 1. Clone this repository

```bash
git clone <your-repo-url>
cd bvp-jobs-scraper
```

### 2. Create Airtable Personal Access Token

1. Go to https://airtable.com/create/tokens
2. Click "Create new token"
3. Name it: "BVP Jobs Integration"
4. Add scopes: `data.records:read`, `data.records:write`
5. Add access to base: "Portfolio Jobs Intelligence"
6. Copy the token

### 3. Set up GitHub Secret

1. Go to your repository Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `AIRTABLE_TOKEN`
4. Value: Paste your Airtable token
5. Click "Add secret"

## Manual Run

To run locally:

```bash
# Install dependencies
pip install requests pandas pyairtable

# Run scraper
python bvp_jobs_analyzer.py

# Load to Airtable
export AIRTABLE_TOKEN="your-token-here"
python load_jobs_to_airtable.py
```

## Automated Schedule

The workflow runs automatically every Monday at 9:00 AM EST via GitHub Actions.

You can also trigger it manually:
1. Go to Actions tab in GitHub
2. Select "BVP Jobs Scraper - Weekly Run"
3. Click "Run workflow"

## Airtable Structure

The system populates 5 tables:

1. **Jobs** - All 5,800+ job listings
2. **Function Analytics** - Summary by function/department
3. **Company Analytics** - Top 50 companies by job count
4. **Weekly Snapshots** - Historical trend tracking
5. **Talent Pooling Opportunities** - Roles open at 2+ companies

## Data Fields

### Jobs Table
- Job Title
- Company
- Function (normalized)
- Level (Executive, Director, Senior, Mid-Level, Junior)
- Location
- Remote (Yes/Hybrid/No)
- URL
- Last Updated

## Customization

To change the schedule, edit `.github/workflows/weekly-scraper.yml`:

```yaml
schedule:
  - cron: '0 14 * * 1'  # Monday at 9 AM EST
```

Cron syntax: `minute hour day-of-month month day-of-week`

## Maintenance

The system automatically:
- Clears old job listings before each upload
- Updates analytics tables
- Creates weekly snapshots for trend analysis
- Saves CSV artifacts for 30 days

## Support

For issues or questions, contact the BVP Talent team.
