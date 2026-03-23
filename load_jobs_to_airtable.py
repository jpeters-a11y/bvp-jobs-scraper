import pandas as pd
from datetime import datetime
import time
import os
from pyairtable import Api

# Import the inference function from the scraper
import sys
sys.path.insert(0, os.path.dirname(__file__))
from bvp_jobs_analyzer import infer_function_from_title

BASE_ID = 'appKRyK4KfiGX9ojv'
PERSONAL_ACCESS_TOKEN = os.environ.get('AIRTABLE_TOKEN', 'YOUR_AIRTABLE_PERSONAL_ACCESS_TOKEN')

# Table IDs
JOBS_TABLE = 'tblHHC9JcSHscBn6S'
COMPANY_ANALYTICS_TABLE = 'tbl1DjAksyFMLl5XZ'
FUNCTION_ANALYTICS_TABLE = 'tbl3ofnnggrF39E2A'
WEEKLY_SNAPSHOTS_TABLE = 'tbl24qL4zM3cZ9QDr'
TALENT_POOLING_TABLE = 'tblnIBdiR7P5mmUn7'

VALID = {'Engineering', 'Sales', 'Marketing', 'Product', 'Design', 'Data & Analytics',
         'Customer Success', 'Operations', 'People & Talent', 'Finance',
         'Legal & Compliance', 'IT', 'Strategy & Business Development',
         'Professional Services', 'Unknown'}

# COMPLETE systematic mapping of all 407 department variations
MAPPING = {
    # PRODUCT
    'Product Management': 'Product', 'Product Engineering': 'Product',
    'Product Design': 'Product', 'Product Solutions': 'Product',
    'Product & Engineering': 'Product', 'Engineering & Design - Product': 'Product',
    'Product & Design': 'Product', 'Product ': 'Product',
    'Product Development & Design': 'Product', 'Enterprise Product': 'Product',
    'Product Experience': 'Product', 'Product, Research, and UI/UX': 'Product',
    'Product Development ': 'Product',

    # ENGINEERING
    'AI Research & Engineering': 'Engineering', 'Security': 'Engineering',
    'R&D': 'Engineering', 'AI': 'Engineering', 'AI Group': 'Engineering',
    'AIT': 'Engineering', 'Engineering Org': 'Engineering',
    'Development': 'Engineering', 'Software Engineering': 'Engineering',
    'Software Development': 'Engineering', 'Core Engineering': 'Engineering',
    'Engineering & Data': 'Engineering', 'Core Tech Engineering': 'Engineering',
    'Software Engineering - Infrastructure': 'Engineering',
    'Hardware': 'Engineering', 'Hardware & Hardware Operations': 'Engineering',
    'Future Propulsion': 'Engineering', 'Robotics & Solutions': 'Engineering',
    'Avionics Communication Power': 'Engineering', 'Nyx Moon & Future Systems': 'Engineering',
    'Digital Systems': 'Engineering', 'Software and Algorithms': 'Engineering',
    'Legal Engineering': 'Engineering', 'Measure Engineering': 'Engineering',
    'ML Engineering': 'Engineering', 'Technical Program Management ': 'Engineering',
    'Technical Program Management': 'Engineering', 'Mobile': 'Engineering',
    'SDKs': 'Engineering', 'Integrations Engineering': 'Engineering',
    'Vehicle Design': 'Engineering', 'Propulsion': 'Engineering',
    'Release Engineering': 'Engineering', 'Hardware Engineering': 'Engineering',
    'Platform Engineering': 'Engineering', 'Platform Engineering ': 'Engineering',
    'Manufacturing Engineering - WA': 'Engineering', 'Developers': 'Engineering',
    'Flight Software': 'Engineering', 'Compute': 'Engineering',
    'Platform & Infrastructure': 'Engineering', 'Game Development Hub': 'Engineering',
    'Cloud Platform Engineering': 'Engineering', 'Engineering & Product': 'Engineering',
    'Quantum Architecture': 'Engineering', 'Game Tech': 'Engineering',
    'Hardware & Manufacturing': 'Engineering', 'Broker Dealer Engineering': 'Engineering',
    'System': 'Engineering', 'Voice Tech': 'Engineering',
    'Information & Security': 'Engineering', 'Data Platform Engineering': 'Engineering',
    'AI Engineering': 'Engineering', 'API Core': 'Engineering',
    'Battery Performance': 'Engineering', 'Mechanisms': 'Engineering',
    'MAIT': 'Engineering', 'Deployed': 'Engineering', 'Security ': 'Engineering',
    'Technology - Software Engineering, Infrastructure, and Security': 'Engineering',
    'Infrastructure and Security': 'Engineering', 'Flight Computing': 'Engineering',

    # SALES
    'Go-To-Market': 'Sales', 'Commercial': 'Sales', 'Account Executives': 'Sales',
    'APAC': 'Sales', 'Mexico': 'Sales', 'Go To Market (GTM)': 'Sales',
    'GTM': 'Sales', 'Revenue': 'Sales', 'Revenue ': 'Sales',
    'Account Management': 'Sales', 'Sales Growth': 'Sales',
    'Sales Business Systems': 'Sales', 'EMEA PSF': 'Sales',
    'Americas Class Hires, Client Solutions': 'Sales', 'Field Sales': 'Sales',
    'Sales Account Executive': 'Sales', 'Leads': 'Sales',
    'Enterprise Sales': 'Sales', 'Sales & Success': 'Sales',
    'B2B Sales': 'Sales', 'Sales Development': 'Sales',
    'Go-to-Market': 'Sales', 'Sales Management': 'Sales',
    'SDR': 'Sales', 'Sales & Account Management': 'Sales',
    'Sales and Go-To-Market (GTM)': 'Sales', 'Sales - Account Executive': 'Sales',
    'Sales Enterprise': 'Sales', 'Strategic Sales': 'Sales',
    'Sales - Sales Engineering': 'Sales', 'Alliances and Growth': 'Sales',
    'CEMI BD & Sales': 'Sales', 'Global FS': 'Sales',

    # CUSTOMER SUCCESS
    'Customer Experience': 'Customer Success', 'Customer Support': 'Customer Success',
    'Customer Success & Solutions': 'Customer Success', 'Customer': 'Customer Success',
    'Support Services': 'Customer Success', 'Client Success & Solutions': 'Customer Success',
    'Solutions Engineering': 'Customer Success', 'Customers': 'Customer Success',
    'Support': 'Customer Success', 'Client Services': 'Customer Success',
    'Client Impact': 'Customer Success', 'Partner Success - Clinical Success': 'Customer Success',
    'Delivery & Enterprise Solutions': 'Customer Success', 'Solutions': 'Customer Success',
    'Customer Service Team': 'Customer Success', 'Solutions ': 'Customer Success',
    'Customer Enablement': 'Customer Success', 'Success': 'Customer Success',
    'Buyer Success': 'Customer Success', 'Partner Success - Enterprise and Commercial ': 'Customer Success',
    'Customer Experience ': 'Customer Success', 'Solution Consultants': 'Customer Success',
    'Client Support': 'Customer Success', 'Customer Care': 'Customer Success',
    'Member Experience': 'Customer Success', 'Customer Success ': 'Customer Success',
    'Customer Success Management': 'Customer Success', 'Customer Team': 'Customer Success',
    'Member Implementation': 'Customer Success', 'Member Onboarding': 'Customer Success',
    'Customer Onboarding and Enablement': 'Customer Success', 'Customer Onboarding': 'Customer Success',
    'Implementation': 'Customer Success', 'Implementation Success': 'Customer Success',
    'Implementation - Nursing': 'Customer Success', 'Provider Support': 'Customer Success',
    'Client Experience': 'Customer Success', 'Client Transitions': 'Customer Success',
    'Implementations': 'Customer Success', 'Partner Success - Key': 'Customer Success',
    'Partner Success': 'Customer Success', 'Technical Account Management': 'Customer Success',
    'Solutions Engineering ': 'Customer Success', 'SMB & Customer Success': 'Customer Success',
    'Advisor Onboarding': 'Customer Success',

    # MARKETING
    'Marketing ': 'Marketing', 'Growth': 'Marketing', 'Marketing Org': 'Marketing',
    'Communications & Brand': 'Marketing', 'Product Marketing': 'Marketing',
    'Product Marketing ': 'Marketing', 'Search': 'Marketing',
    'Advertising Solutions': 'Marketing', 'Growth and Marketing': 'Marketing',
    'Marketing Operations': 'Marketing', 'Marketing Development': 'Marketing',
    'Marketing Tech': 'Marketing', 'Marketing & Sales': 'Marketing',
    'Demand Generation': 'Marketing', 'Advertising': 'Marketing',
    'Social Gaming + Developer Experience': 'Marketing', 'Artists & Curation': 'Marketing',
    'Brand Design': 'Marketing', 'Creatives Studio': 'Marketing',

    # DATA & ANALYTICS
    'Data': 'Data & Analytics', 'Data Science': 'Data & Analytics',
    'Data & Analytics Org': 'Data & Analytics', 'Data Science & Engineering': 'Data & Analytics',
    'Blockchain Intelligence': 'Data & Analytics', 'Research': 'Data & Analytics',
    'Analytics': 'Data & Analytics', 'Data Operations': 'Data & Analytics',
    'Research, Analytics & Data Science': 'Data & Analytics',
    'Data Science (DS)': 'Data & Analytics', 'Data & ML': 'Data & Analytics',
    'Defcon - Analytics': 'Data & Analytics', 'OR/Analytics': 'Data & Analytics',
    'Data Science & Analytics': 'Data & Analytics', 'Defcon AI': 'Data & Analytics',
    'Data / AI': 'Data & Analytics', 'Ops Analytics ': 'Data & Analytics',
    'Science': 'Data & Analytics', 'Druid Query': 'Data & Analytics',

    # OPERATIONS
    'Clinical': 'Operations', 'Clinical Operations': 'Operations',
    'G&A': 'Operations', 'Revenue Operations': 'Operations',
    'Business Operations': 'Operations', 'Finance & Business Operations': 'Operations',
    'People Operations': 'Operations', 'Finance Operations': 'Operations',
    'Strategy & Operations': 'Operations', 'Global Operations ': 'Operations',
    'Core Operations': 'Operations', 'Product Operations ': 'Operations',
    'R&D Operations': 'Operations', 'People/Office Operations': 'Operations',
    'Operations ': 'Operations', 'Medical Operations': 'Operations',
    'Client Operations': 'Operations', 'Central Operations': 'Operations',
    'Partner Ops': 'Operations', 'Operations & Analytics ': 'Operations',
    'Business Operations ': 'Operations', 'RevOps': 'Operations',
    'Strategy Operations': 'Operations', 'Sales Strategy & Operations': 'Operations',
    'Logistics': 'Operations', 'Education & Training': 'Operations',
    'Ecosystems & Channels': 'Operations', 'Workplace': 'Operations',
    'Launch': 'Operations', 'Workplace Experience and Travel': 'Operations',
    'EHS&S': 'Operations', 'Manufacturing & Quality': 'Operations',
    'Quality and Product Assurance': 'Operations', 'Quality Assurance': 'Operations',
    'Quality - CA': 'Operations', 'Mission Operations and Flight Dynamics': 'Operations',
    'Business Tech Track': 'Operations', 'Procurement': 'Operations',
    'Procurement ': 'Operations', 'Ecosystems': 'Operations',
    'Tech & Operations': 'Operations', 'Supply Chain & Sourcing': 'Operations',
    'Supply Success': 'Operations', 'Supply Growth': 'Operations',
    'Precision': 'Operations', 'General & Administrative': 'Operations',
    'General and Administrative': 'Operations', 'Administration': 'Operations',
    'Program': 'Operations', 'Safeguards (Trust & Safety) ': 'Operations',
    'Trust & Safety': 'Operations', 'Abuse Protection': 'Operations',
    'Pre-Litigation': 'Operations', 'Risk & Compliance': 'Operations',
    'Risk': 'Operations', 'Compliance': 'Operations', 'Corporate Office | 0700': 'Operations',
    'FCRM - Compliance | 0220': 'Operations', 'Adeptus': 'Operations',
    'Strategy, Operations & Data': 'Operations',

    # PEOPLE & TALENT
    'People': 'People & Talent', 'Talent': 'People & Talent',
    'Talent Acquisition': 'People & Talent', 'Recruiting': 'People & Talent',
    'People Team': 'People & Talent', 'People Org': 'People & Talent',
    'People & Culture': 'People & Talent', 'People ': 'People & Talent',
    'People Ops': 'People & Talent', 'People Enablement': 'People & Talent',
    'People Business Partnering': 'People & Talent', 'Human Resources': 'People & Talent',
    'People Ops & Talent': 'People & Talent', 'HR & People': 'People & Talent',
    'Talent Development': 'People & Talent', 'People & Performance': 'People & Talent',

    # FINANCE
    'Accounting': 'Finance', 'Finance ': 'Finance', 'FP&A': 'Finance',
    'Finance Org': 'Finance', 'Finance & IT': 'Finance',
    'Finance, Accounting & Strategy': 'Finance', 'Treasury': 'Finance',
    'Abacus!': 'Finance', 'Reid Accountants + Advisors': 'Finance',
    'AbitOs Accountants + Advisors': 'Finance', 'RRBB Accountants + Advisors': 'Finance',
    'Crete Professionals Alliance': 'Finance', 'Wealth Management': 'Finance',
    'Cutler Advisors': 'Finance', 'Larson Gross': 'Finance',
    'TKR Accountants & Advisors': 'Finance', 'Levy, Erlanger & Company': 'Finance',
    'Breslow Starling Accountants + Advisors': 'Finance', 'Credit': 'Finance',
    'Investing & Capital Markets': 'Finance', 'Brokerage and Custody': 'Finance',
    'Banking': 'Finance', 'Corporate Development': 'Finance',
    'FP&A Tech': 'Finance', 'Employee Benefits Team': 'Finance',
    'Financial Advice': 'Finance', 'Financial Institutions Team': 'Finance',
    'Wholesale Benefits Team': 'Finance', 'Retirement Services Team': 'Finance',
    'Private Client Services Team': 'Finance', 'Finance & Legal': 'Finance',

    # STRATEGY
    'Strategy & BD': 'Strategy & Business Development',
    'Partnerships': 'Strategy & Business Development',
    'Business Development': 'Strategy & Business Development',
    'Strategy': 'Strategy & Business Development',
    'Corporate & Business Development Strategy': 'Strategy & Business Development',
    'Strategy & Org': 'Strategy & Business Development',

    # LEGAL
    'Legal': 'Legal & Compliance', 'Legal ': 'Legal & Compliance',
    'Legal Counsel': 'Legal & Compliance', 'Legal - Product': 'Legal & Compliance',
    'Global Law': 'Legal & Compliance', 'Policy': 'Legal & Compliance',
    'AI Policy & Societal Impacts': 'Legal & Compliance',

    # IT
    'Technology': 'IT', 'Technology ': 'IT', 'AIOA': 'IT',
    'FCRM - Data Tech | 0240': 'IT', 'Corporate Technology': 'IT',
    'Business Systems': 'IT', 'Business Technologies': 'IT',
    'Enterprise Cloud Applications': 'IT',

    # PROFESSIONAL SERVICES
    'Global PSF': 'Professional Services',
    'Product Management, Support, & Operations': 'Professional Services',

    # DESIGN
    'UX Design': 'Design', 'UX': 'Design', 'Design ': 'Design',
    'Creative': 'Design', 'Content Design': 'Design',
}


def normalize(func):
    """Normalize function name using mapping, return None if unmapped."""
    if func in VALID:
        return func
    mapped = MAPPING.get(func)
    if mapped:
        return mapped
    return None  # Signal that we need to infer from title


def clear_table(table):
    """Delete all records from a table."""
    existing = table.all()
    if existing:
        batch_ids = [r['id'] for r in existing]
        for i in range(0, len(batch_ids), 10):
            table.batch_delete(batch_ids[i:i+10])
            time.sleep(0.2)
    return len(existing)


def upload_jobs(df, api):
    """Clear and re-upload all jobs to the Jobs table."""
    print("\n" + "=" * 60)
    print("STEP 1: Upload Jobs")
    print("=" * 60)

    jobs_table = api.table(BASE_ID, JOBS_TABLE)

    print("Clearing existing records...")
    cleared = clear_table(jobs_table)
    print(f"  Cleared {cleared} old records")

    print(f"Uploading {len(df)} jobs...")
    total_batches = (len(df) + 9) // 10
    for i in range(0, len(df), 10):
        records = []
        for _, row in df.iloc[i:i+10].iterrows():
            records.append({
                'Job Title': str(row['Title']),
                'Company': str(row['Company']),
                'Function': str(row['Fixed']),
                'Level': str(row['Level']),
                'Location': str(row['Location']),
                'Remote': str(row['Remote']),
                'URL': str(row['URL']) if pd.notna(row['URL']) else '',
                'Last Updated': datetime.now().isoformat()
            })
        jobs_table.batch_create(records)
        batch_num = i // 10 + 1
        if batch_num % 100 == 0:
            print(f"  Batch {batch_num}/{total_batches}")
        time.sleep(0.2)

    print(f"✅ Jobs uploaded! ({len(df[df['Fixed'] == 'Unknown'])} Unknown)")


def update_function_analytics(df, api):
    """Clear and recreate function analytics."""
    print("\n" + "=" * 60)
    print("STEP 2: Function Analytics")
    print("=" * 60)

    table = api.table(BASE_ID, FUNCTION_ANALYTICS_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    total_jobs = len(df)
    func_groups = df.groupby('Fixed')

    records = []
    for func_name, group in func_groups:
        remote_count = len(group[group['Remote'] == 'Yes'])
        records.append({
            'Function': str(func_name),
            'Total Jobs': int(len(group)),
            'Percentage of Total': len(group) / total_jobs if total_jobs > 0 else 0,
            'Remote Jobs': int(remote_count),
            'Remote Percentage': remote_count / len(group) if len(group) > 0 else 0,
            'Companies Hiring': int(group['Company'].nunique()),
            'Executive Roles': int(len(group[group['Level'] == 'Executive'])),
            'Senior Roles': int(len(group[group['Level'] == 'Senior'])),
            'Last Updated': datetime.now().isoformat()
        })

    # Upload in batches of 10
    for i in range(0, len(records), 10):
        table.batch_create(records[i:i+10])
        time.sleep(0.2)

    print(f"✅ Created {len(records)} function analytics records")


def update_company_analytics(df, api):
    """Clear and recreate company analytics (top 50 by job count)."""
    print("\n" + "=" * 60)
    print("STEP 3: Company Analytics")
    print("=" * 60)

    table = api.table(BASE_ID, COMPANY_ANALYTICS_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    company_groups = df.groupby('Company')
    company_stats = []

    for company, group in company_groups:
        remote_count = len(group[group['Remote'] == 'Yes'])
        company_stats.append({
            'company': str(company),
            'total': len(group),
            'engineering': int(len(group[group['Fixed'] == 'Engineering'])),
            'sales': int(len(group[group['Fixed'] == 'Sales'])),
            'marketing': int(len(group[group['Fixed'] == 'Marketing'])),
            'product': int(len(group[group['Fixed'] == 'Product'])),
            'remote_pct': remote_count / len(group) if len(group) > 0 else 0,
            'unique_functions': int(group['Fixed'].nunique()),
        })

    # Sort by total jobs descending, take top 50
    company_stats.sort(key=lambda x: x['total'], reverse=True)
    top_50 = company_stats[:50]

    records = []
    for cs in top_50:
        records.append({
            'Company Name': cs['company'],
            'Total Jobs': int(cs['total']),
            'Engineering Jobs': cs['engineering'],
            'Sales Jobs': cs['sales'],
            'Marketing Jobs': cs['marketing'],
            'Product Jobs': cs['product'],
            'Remote Percentage': cs['remote_pct'],
            'Unique Functions': cs['unique_functions'],
            'Last Updated': datetime.now().isoformat()
        })

    for i in range(0, len(records), 10):
        table.batch_create(records[i:i+10])
        time.sleep(0.2)

    print(f"✅ Created {len(records)} company analytics records")


def create_weekly_snapshot(df, api):
    """Add a new weekly snapshot record (append, don't clear)."""
    print("\n" + "=" * 60)
    print("STEP 4: Weekly Snapshot")
    print("=" * 60)

    table = api.table(BASE_ID, WEEKLY_SNAPSHOTS_TABLE)

    remote_count = len(df[df['Remote'] == 'Yes'])
    snapshot = {
        'Snapshot Date': datetime.now().strftime('%Y-%m-%d'),
        'Total Jobs': int(len(df)),
        'Total Companies Hiring': int(df['Company'].nunique()),
        'Engineering Jobs': int(len(df[df['Fixed'] == 'Engineering'])),
        'Sales Jobs': int(len(df[df['Fixed'] == 'Sales'])),
        'Marketing Jobs': int(len(df[df['Fixed'] == 'Marketing'])),
        'Product Jobs': int(len(df[df['Fixed'] == 'Product'])),
        'Remote Jobs': int(remote_count),
        'Remote Percentage': remote_count / len(df) if len(df) > 0 else 0,
        'Notes': 'Automated weekly update'
    }

    table.create(snapshot)
    print(f"✅ Snapshot created for {snapshot['Snapshot Date']}: {snapshot['Total Jobs']} jobs across {snapshot['Total Companies Hiring']} companies")


def update_talent_pooling(df, api):
    """Clear and recreate talent pooling opportunities."""
    print("\n" + "=" * 60)
    print("STEP 5: Talent Pooling Opportunities")
    print("=" * 60)

    table = api.table(BASE_ID, TALENT_POOLING_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    # Group by job title
    title_groups = df.groupby('Title')

    opportunities = []
    for title, group in title_groups:
        companies = sorted(set(group['Company']))
        num_companies = len(companies)

        if num_companies >= 2:
            # Safe mode() extraction
            func_mode = group['Fixed'].mode()
            func = str(func_mode.iloc[0]) if len(func_mode) > 0 else 'Unknown'

            level_mode = group['Level'].mode()
            level = str(level_mode.iloc[0]) if len(level_mode) > 0 else 'Unknown'

            total_openings = int(len(group))

            if num_companies >= 5:
                priority = 'High (5+ companies)'
            elif num_companies >= 3:
                priority = 'Medium (3-4 companies)'
            else:
                priority = 'Low (2 companies)'

            opportunities.append({
                'Job Title': str(title),
                'Number of Companies': int(num_companies),
                'Total Openings': total_openings,
                'Companies': ', '.join(companies),
                'Function': func,
                'Level': level,
                'Priority': priority,
                'Last Updated': datetime.now().isoformat()
            })

    # Sort by number of companies descending
    opportunities.sort(key=lambda x: x['Number of Companies'], reverse=True)

    # Upload in batches
    uploaded = 0
    for i in range(0, len(opportunities), 10):
        table.batch_create(opportunities[i:i+10])
        uploaded += len(opportunities[i:i+10])
        if (i // 10 + 1) % 50 == 0:
            print(f"  Uploaded {uploaded}/{len(opportunities)} opportunities...")
        time.sleep(0.2)

    print(f"✅ Created {len(opportunities)} talent pooling opportunities")


def main():
    print("=" * 60)
    print("BVP PORTFOLIO JOBS INTELLIGENCE - FULL PIPELINE")
    print("=" * 60)

    if PERSONAL_ACCESS_TOKEN == 'YOUR_AIRTABLE_PERSONAL_ACCESS_TOKEN':
        print("ERROR: Set AIRTABLE_TOKEN environment variable")
        return

    api = Api(PERSONAL_ACCESS_TOKEN)

    # ── Load and normalize CSV ──────────────────────────────
    print("\nLoading CSV data...")
    df = pd.read_csv('bvp_jobs_analysis.csv')
    print(f"Found {len(df)} jobs ({len(df[df['Function'] == 'Unknown'])} Unknown in CSV)")

    # First pass: use mapping
    df['Fixed'] = df['Function'].apply(normalize)

    # Second pass: infer from title for unmapped values
    needs_inference = df['Fixed'].isna()
    print(f"Inferring from title for {needs_inference.sum()} unmapped departments...")
    df.loc[needs_inference, 'Fixed'] = df.loc[needs_inference, 'Title'].apply(infer_function_from_title)
    print(f"After normalization: {len(df[df['Fixed'] == 'Unknown'])} Unknown ({len(df[df['Fixed'] == 'Unknown'])/len(df)*100:.1f}%)")

    # Show unmapped departments for future mapping improvements
    unmapped = df[(df['Function'] != 'Unknown') & (df['Fixed'] == 'Unknown')]
    if len(unmapped) > 0:
        print(f"\nUnmapped departments ({len(unmapped)} jobs):")
        print(unmapped['Function'].value_counts().head(20))
    else:
        print("\n✅ All non-Unknown values successfully mapped!")

    # ── Run full pipeline ───────────────────────────────────
    upload_jobs(df, api)
    update_function_analytics(df, api)
    update_company_analytics(df, api)
    create_weekly_snapshot(df, api)
    update_talent_pooling(df, api)

    print("\n" + "=" * 60)
    print("✅ ALL TABLES UPDATED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\nBase URL: https://airtable.com/{BASE_ID}")
    print(f"\nTables updated:")
    print(f"  • Jobs - {len(df)} listings")
    print(f"  • Function Analytics - by function summary")
    print(f"  • Company Analytics - top 50 companies")
    print(f"  • Weekly Snapshots - new snapshot added")
    print(f"  • Talent Pooling - multi-company opportunities")


if __name__ == "__main__":
    main()
