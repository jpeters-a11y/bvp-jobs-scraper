import pandas as pd
import re
from collections import Counter
from datetime import datetime, timezone
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

# ── FILTERING CONFIGURATION ────────────────────────────────
# Companies to exclude entirely (too many irrelevant local-market roles)
EXCLUDED_COMPANIES = {
    'Toss',
}

# Regex: CJK + Korean Hangul/Jamo character ranges
NON_LATIN_RE = re.compile(
    r'[\u3000-\u9FFF\uAC00-\uD7AF\u1100-\u11FF\uA960-\uA97F\uD7B0-\uD7FF]'
)

def is_non_english_title(title):
    """True if >50% of alphabetic chars are non-Latin (CJK/Korean)."""
    if not isinstance(title, str):
        return False
    non_latin = len(NON_LATIN_RE.findall(title))
    alpha = sum(1 for c in title if c.isalpha())
    if alpha == 0:
        return True
    return non_latin / alpha > 0.5

def is_test_job(title):
    """True if title looks like a test/placeholder posting."""
    if not isinstance(title, str):
        return False
    lower = title.lower()
    return '[test]' in lower or 'test job' in lower or 'test department' in lower


# ── BVP ROADMAP MAPPING ────────────────────────────────────
# Maps portfolio company names → BVP investing roadmap.
# Sourced from IR <> Talent Sync (Talent Portal Internal).
ROADMAP_MAP = {
    # AI
    'Anthropic': 'AI', 'Perplexity AI': 'AI', 'DeepL': 'AI',
    'Fal': 'AI', 'Writer': 'AI', 'Typeface': 'AI',
    'Jasper': 'AI', 'Harvey': 'AI', 'Sierra': 'AI',
    # Robotics
    'Waymo': 'Robotics', 'Halter': 'Robotics', 'Auterion': 'Robotics',
    # Cybersecurity
    'Team8': 'Cybersecurity', 'Upwind Security': 'Cybersecurity',
    'Torq': 'Cybersecurity', 'BigID': 'Cybersecurity',
    'Axonius': 'Cybersecurity', 'Claroty': 'Cybersecurity',
    'Doppel': 'Cybersecurity', 'Forter': 'Cybersecurity',
    'Teleport': 'Cybersecurity',
    # Horizontal SaaS
    'Canva': 'Horizontal SaaS', 'Intercom': 'Horizontal SaaS',
    'Hibob': 'Horizontal SaaS', 'Yotpo': 'Horizontal SaaS',
    'Pocus': 'Horizontal SaaS', 'Papaya Global': 'Horizontal SaaS',
    'ManyChat': 'Horizontal SaaS', 'Vertice': 'Horizontal SaaS',
    # Vertical SaaS
    'MaintainX': 'Vertical SaaS', 'Legora': 'Vertical SaaS',
    'EliseAI': 'Vertical SaaS', 'EvenUp': 'Vertical SaaS',
    'Fieldguide': 'Vertical SaaS', 'Restaurant365': 'Vertical SaaS',
    'Shopmonkey': 'Vertical SaaS', 'brightwheel': 'Vertical SaaS',
    'DroneDeploy': 'Vertical SaaS', 'Aiwyn': 'Vertical SaaS',
    'Unframe AI': 'Vertical SaaS', 'Curri': 'Vertical SaaS',
    # Fintech
    'Ramp': 'Fintech', 'TRM Labs': 'Fintech', 'Mambu': 'Fintech',
    'Betterment': 'Fintech', 'Melio': 'Fintech', 'Thunes': 'Fintech',
    'Easebuzz': 'Fintech', 'Farther': 'Fintech',
    'CaptivateIQ': 'Fintech', 'Pave': 'Fintech',
    'Crete Professionals Alliance': 'Fintech',
    'Carr, Riggs & Ingram': 'Fintech',
    # Data / Cloud Infrastructure
    'ClickHouse': 'Data / Cloud Infrastructure',
    'DRIVENETS': 'Data / Cloud Infrastructure',
    # Developer Platforms
    'LaunchDarkly': 'Developer Platforms',
    'Cloudinary': 'Developer Platforms',
    'Port IO': 'Developer Platforms',
    # Healthcare
    'Abridge': 'Healthcare', 'Groups Recover Together': 'Healthcare',
    'MediBuddy': 'Healthcare', 'Qventus': 'Healthcare',
    # Consumer
    'Discord': 'Consumer', 'Cambly': 'Consumer',
    'Klook': 'Consumer', 'Livspace': 'Consumer',
    # Marketplaces
    'carwow': 'Marketplaces', 'GLG': 'Marketplaces',
    # Deep Tech
    'The Exploration Company': 'Deep Tech',
    'Sila Nanotechnologies': 'Deep Tech',
}


# ── FUNCTION NORMALIZATION ──────────────────────────────────
VALID = {
    'Engineering', 'Sales', 'Marketing', 'Product', 'Design',
    'Data & Analytics', 'Customer Success', 'Operations',
    'People & Talent', 'Finance', 'Legal & Compliance', 'IT',
    'Strategy & Business Development', 'Professional Services',
    'Clinical', 'Unknown'
}

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
    'Clinical': 'Clinical',
    'G&A': 'Operations', 'Revenue Operations': 'Operations',
    'Business Operations': 'Operations', 'Finance & Business Operations': 'Operations',
    'People Operations': 'Operations', 'Finance Operations': 'Operations',
    'Strategy & Operations': 'Operations', 'Global Operations ': 'Operations',
    'Core Operations': 'Operations', 'Product Operations ': 'Operations',
    'R&D Operations': 'Operations', 'People/Office Operations': 'Operations',
    'Operations ': 'Operations',
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
    'Risk': 'Operations', 'Compliance': 'Operations',
    'Corporate Office | 0700': 'Operations',
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

    # CLINICAL (new category for healthcare delivery roles)
    'General Cardiology': 'Clinical', 'Interventional Cardiology': 'Clinical',
    'Clinical Operations': 'Clinical', 'Insurance Operations': 'Clinical',
    'Medical Operations': 'Clinical',

    # ── Additional mappings from unmapped dept analysis (2026-03-23) ──

    # Waymo coded departments → Engineering
    'Perception (7LT)': 'Engineering', 'Safety (7GB)': 'Engineering',
    'CSI (7LV)': 'Engineering', 'FleetOps: Dev Rdns (7QI)': 'Engineering',
    'Planner (7LU)': 'Engineering', 'AI Foundations (7SJ)': 'Engineering',
    'Ops Center (9QB)': 'Engineering', 'RMC Direct Opex (78K)': 'Engineering',
    'Pipeline (N/A)': 'Engineering',

    # Anthropic departments
    'AI Public Policy & Societal Impacts': 'Legal & Compliance',

    # Straightforward mappings
    'Channel Sales': 'Sales',
    'Engagement': 'Marketing',
    'Information Technology': 'IT',
    'Client Success': 'Customer Success',
    'Strategy and Operations': 'Operations',
    'Global Investigations': 'Legal & Compliance',
    'Creator': 'Marketing',
}


def enhanced_infer_function(title):
    """Wrap the scraper's inference with additional patterns."""
    if not isinstance(title, str):
        return 'Unknown'
    lower = title.lower()

    # Clinical patterns (check first — these are specific)
    clinical_kw = [
        'counselor', 'therapist', 'clinician', 'nurse', 'physician',
        'cardiology', 'cardiologist', 'medical director', 'psychiatr',
        'substance use', 'behavioral health', 'clinical supervisor',
        'pharmacy', 'pharmacist', 'dietitian', 'nutritionist',
        'social worker', 'case manager', 'care coordinator',
        'insurance advisor',
    ]
    if any(kw in lower for kw in clinical_kw):
        return 'Clinical'

    # Additional Engineering patterns
    eng_kw = ['dba', 'devops', 'sre ', 'site reliability',
              'tech lead', 'network technician', 'security researcher',
              'security analyst', 'ml manager', 'machine learning']
    if any(kw in lower for kw in eng_kw):
        return 'Engineering'

    # Additional Design patterns
    if 'interior designer' in lower or 'graphic designer' in lower:
        return 'Design'

    # Additional Operations patterns
    ops_kw = ['general affairs', 'purchasing manager', 'barista',
              'merchandising', 'validation manager', 'regional manager',
              'operations lead']
    if any(kw in lower for kw in ops_kw):
        return 'Operations'

    # Additional Finance patterns
    fin_kw = ['tax ', 'tax,', 'auditor', 'bookkeep', 'controller',
              'accounts payable', 'accounts receivable', 'cpa', 'cfo']
    if any(kw in lower for kw in fin_kw):
        return 'Finance'

    # Additional Sales patterns
    if 'business manager - sales' in lower or 'project executive' in lower:
        return 'Sales'

    # Additional IT patterns
    if 'it manager' in lower or 'it sox' in lower or 'idc network' in lower:
        return 'IT'

    # Additional Legal patterns
    if 'aml ' in lower or 'kyc ' in lower or 'compliance' in lower:
        return 'Legal & Compliance'

    # Additional Customer Success
    if 'customer hero' in lower or 'customer protection' in lower:
        return 'Customer Success'

    # Fall back to the scraper's inference
    return infer_function_from_title(title)


def normalize(func):
    if func in VALID:
        return func
    mapped = MAPPING.get(func)
    if mapped:
        return mapped
    return None


def clear_table(table):
    existing = table.all()
    if existing:
        ids = [r['id'] for r in existing]
        for i in range(0, len(ids), 10):
            table.batch_delete(ids[i:i + 10])
            time.sleep(0.2)
    return len(existing)


def get_previous_company_totals(api):
    """Read current Company Analytics to capture last week's job counts."""
    table = api.table(BASE_ID, COMPANY_ANALYTICS_TABLE)
    records = table.all()
    return {
        r['fields'].get('Company Name', ''): r['fields'].get('Total Jobs', 0)
        for r in records if r['fields'].get('Company Name')
    }


# ── PIPELINE STEPS ──────────────────────────────────────────

def upload_jobs(df, api):
    print("\n" + "=" * 60)
    print("STEP 1: Upload Jobs")
    print("=" * 60)

    table = api.table(BASE_ID, JOBS_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    print(f"Uploading {len(df)} jobs...")
    total_batches = (len(df) + 9) // 10
    for i in range(0, len(df), 10):
        records = []
        for _, row in df.iloc[i:i + 10].iterrows():
            records.append({
                'Job Title': str(row['Title']),
                'Company': str(row['Company']),
                'Function': str(row['Fixed']),
                'Level': str(row['Level']),
                'Location': str(row['Location']),
                'Remote': str(row['Remote']),
                'URL': str(row['URL']) if pd.notna(row['URL']) else '',
                'Last Updated': datetime.now(timezone.utc).isoformat()
            })
        table.batch_create(records)
        batch_num = i // 10 + 1
        if batch_num % 100 == 0:
            print(f"  Batch {batch_num}/{total_batches}")
        time.sleep(0.2)

    print(f"✅ Jobs uploaded! ({len(df[df['Fixed'] == 'Unknown'])} Unknown)")


def update_function_analytics(df, api):
    print("\n" + "=" * 60)
    print("STEP 2: Function Analytics")
    print("=" * 60)

    table = api.table(BASE_ID, FUNCTION_ANALYTICS_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    total_jobs = len(df)
    records = []
    for func_name, group in df.groupby('Fixed'):
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
            'Last Updated': datetime.now(timezone.utc).isoformat()
        })

    for i in range(0, len(records), 10):
        table.batch_create(records[i:i + 10])
        time.sleep(0.2)
    print(f"✅ Created {len(records)} function analytics records")


def update_company_analytics(df, api, prev_totals):
    print("\n" + "=" * 60)
    print("STEP 3: Company Analytics (with Roadmap + Velocity)")
    print("=" * 60)

    table = api.table(BASE_ID, COMPANY_ANALYTICS_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    stats = []
    for company, group in df.groupby('Company'):
        remote_count = len(group[group['Remote'] == 'Yes'])
        total = len(group)
        prev = prev_totals.get(company)

        stats.append({
            'company': str(company),
            'total': total,
            'eng': int(len(group[group['Fixed'] == 'Engineering'])),
            'sales': int(len(group[group['Fixed'] == 'Sales'])),
            'mktg': int(len(group[group['Fixed'] == 'Marketing'])),
            'prod': int(len(group[group['Fixed'] == 'Product'])),
            'remote_pct': remote_count / total if total > 0 else 0,
            'funcs': int(group['Fixed'].nunique()),
            'roadmap': ROADMAP_MAP.get(company, ''),
            'prev': prev,
            'wow': total - prev if prev is not None else None,
        })

    stats.sort(key=lambda x: x['total'], reverse=True)
    top_50 = stats[:50]

    records = []
    for s in top_50:
        rec = {
            'Company Name': s['company'],
            'Total Jobs': int(s['total']),
            'Engineering Jobs': s['eng'],
            'Sales Jobs': s['sales'],
            'Marketing Jobs': s['mktg'],
            'Product Jobs': s['prod'],
            'Remote Percentage': s['remote_pct'],
            'Unique Functions': s['funcs'],
            'Last Updated': datetime.now(timezone.utc).isoformat(),
        }
        if s['roadmap']:
            rec['BVP Roadmap'] = s['roadmap']
        if s['prev'] is not None:
            rec['Previous Week Jobs'] = int(s['prev'])
        if s['wow'] is not None:
            rec['WoW Change'] = int(s['wow'])
        records.append(rec)

    for i in range(0, len(records), 10):
        table.batch_create(records[i:i + 10])
        time.sleep(0.2)

    # Print velocity highlights
    movers = [s for s in top_50 if s['wow'] is not None and s['wow'] != 0]
    movers.sort(key=lambda x: x['wow'], reverse=True)
    if movers:
        up = [s for s in movers if s['wow'] > 0]
        down = [s for s in movers if s['wow'] < 0]
        if up:
            print(f"\n  📈 Biggest hiring increases:")
            for s in up[:5]:
                print(f"     {s['company']}: +{s['wow']} jobs ({s['prev']} → {s['total']})")
        if down:
            print(f"  📉 Biggest hiring decreases:")
            for s in down[-5:]:
                print(f"     {s['company']}: {s['wow']} jobs ({s['prev']} → {s['total']})")

    roadmap_count = sum(1 for s in top_50 if s['roadmap'])
    print(f"\n✅ Created {len(records)} company analytics records ({roadmap_count} with roadmap)")


def create_weekly_snapshot(df, api):
    print("\n" + "=" * 60)
    print("STEP 4: Weekly Snapshot")
    print("=" * 60)

    table = api.table(BASE_ID, WEEKLY_SNAPSHOTS_TABLE)
    remote_count = len(df[df['Remote'] == 'Yes'])

    snapshot = {
        'Snapshot Date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
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
    print(f"✅ Snapshot: {snapshot['Snapshot Date']} — {snapshot['Total Jobs']} jobs, {snapshot['Total Companies Hiring']} companies")


def update_talent_pooling(df, api):
    """Cluster demand by Function + Level across the portfolio.

    Instead of grouping by exact job title (which fragments the signal),
    we cluster by Function + Level to answer: "How many portfolio companies
    are hiring Senior Engineers right now?" Within each cluster we surface
    the most common exact titles, companies, remote %, and roadmap
    concentration so the talent team can act on the signal.
    """
    print("\n" + "=" * 60)
    print("STEP 5: Talent Pooling Opportunities (Function + Level clusters)")
    print("=" * 60)

    table = api.table(BASE_ID, TALENT_POOLING_TABLE)
    cleared = clear_table(table)
    print(f"  Cleared {cleared} old records")

    # Filter out Unknown function — not useful for talent pooling
    pool_df = df[df['Fixed'] != 'Unknown'].copy()

    clusters = []
    for (func, level), group in pool_df.groupby(['Fixed', 'Level']):
        companies = sorted(set(group['Company']))
        n_companies = len(companies)

        # Only include clusters with 3+ companies hiring
        if n_companies < 3:
            continue

        total_openings = len(group)
        remote_count = len(group[group['Remote'] == 'Yes'])
        remote_pct = remote_count / total_openings if total_openings > 0 else 0

        # Top 5 most common exact titles in this cluster
        title_counts = group['Title'].value_counts()
        sample_titles = []
        for title, count in title_counts.head(5).items():
            sample_titles.append(f"{title} ({count})")

        # Roadmap concentration: which roadmaps appear most in this cluster
        roadmap_companies = [
            ROADMAP_MAP[c] for c in companies if c in ROADMAP_MAP
        ]
        roadmap_summary = ''
        if roadmap_companies:
            rc = Counter(roadmap_companies).most_common(3)
            roadmap_summary = ', '.join(f"{rm} ({ct})" for rm, ct in rc)

        # Priority based on company count
        if n_companies >= 15:
            priority = 'Critical (15+ companies)'
        elif n_companies >= 10:
            priority = 'High (10-14 companies)'
        elif n_companies >= 5:
            priority = 'Medium (5-9 companies)'
        else:
            priority = 'Low (3-4 companies)'

        # Role Cluster label: "Level Function" e.g. "Senior Engineering"
        cluster_label = f"{level} {func}"

        clusters.append({
            'Role Cluster': cluster_label,
            'Number of Companies': int(n_companies),
            'Total Openings': int(total_openings),
            'Companies': ', '.join(companies),
            'Function': str(func),
            'Level': str(level),
            'Priority': priority,
            'Last Updated': datetime.now(timezone.utc).isoformat(),
            # New fields (require manual creation in Airtable)
            'Sample Titles': '\n'.join(sample_titles),
            'Remote Percentage': remote_pct,
            'Top Roadmaps': roadmap_summary,
        })

    # Sort: highest company count first
    clusters.sort(key=lambda x: x['Number of Companies'], reverse=True)

    # Upload
    uploaded = 0
    for i in range(0, len(clusters), 10):
        table.batch_create(clusters[i:i + 10])
        uploaded += len(clusters[i:i + 10])
        time.sleep(0.2)

    # Print highlights
    print(f"\n  Top demand clusters:")
    for c in clusters[:10]:
        print(f"    {c['Role Cluster']}: {c['Number of Companies']} companies, {c['Total Openings']} openings")

    print(f"\n✅ Created {len(clusters)} talent pooling clusters")


# ── MAIN ────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("BVP PORTFOLIO JOBS INTELLIGENCE - FULL PIPELINE")
    print("=" * 60)

    if PERSONAL_ACCESS_TOKEN == 'YOUR_AIRTABLE_PERSONAL_ACCESS_TOKEN':
        print("ERROR: Set AIRTABLE_TOKEN environment variable")
        return

    api = Api(PERSONAL_ACCESS_TOKEN)

    # Load CSV
    print("\nLoading CSV data...")
    df = pd.read_csv('bvp_jobs_analysis.csv')
    raw = len(df)
    print(f"Raw CSV: {raw} jobs")

    # Filter: excluded companies
    df = df[~df['Company'].isin(EXCLUDED_COMPANIES)]
    n_co = raw - len(df)
    print(f"Excluded companies ({', '.join(EXCLUDED_COMPANIES)}): -{n_co}")

    # Filter: non-English titles
    pre = len(df)
    df = df[~df['Title'].apply(is_non_english_title)]
    n_lang = pre - len(df)
    print(f"Non-English titles: -{n_lang}")

    # Filter: test/junk
    pre = len(df)
    df = df[~df['Title'].apply(is_test_job)]
    n_test = pre - len(df)
    print(f"Test/junk postings: -{n_test}")

    total_filtered = n_co + n_lang + n_test
    print(f"\nAfter filtering: {len(df)} jobs ({total_filtered} removed, {total_filtered/raw*100:.1f}%)")

    # Normalize functions
    df['Fixed'] = df['Function'].apply(normalize)
    needs = df['Fixed'].isna()
    print(f"Inferring from title for {needs.sum()} unmapped departments...")
    df.loc[needs, 'Fixed'] = df.loc[needs, 'Title'].apply(enhanced_infer_function)
    unk = len(df[df['Fixed'] == 'Unknown'])
    print(f"After normalization: {unk} Unknown ({unk/len(df)*100:.1f}%)")

    unmapped = df[(df['Function'] != 'Unknown') & (df['Fixed'] == 'Unknown')]
    if len(unmapped) > 0:
        print(f"\nUnmapped departments ({len(unmapped)} jobs):")
        print(unmapped['Function'].value_counts().head(20))

    # Capture previous week for velocity
    print("\nCapturing previous company totals for velocity tracking...")
    prev_totals = get_previous_company_totals(api)
    print(f"  Found {len(prev_totals)} companies from previous week")

    # Run pipeline
    upload_jobs(df, api)
    update_function_analytics(df, api)
    update_company_analytics(df, api, prev_totals)
    create_weekly_snapshot(df, api)
    update_talent_pooling(df, api)

    # Summary
    roadmap_hits = sum(1 for c in df['Company'].unique() if c in ROADMAP_MAP)
    print("\n" + "=" * 60)
    print("✅ ALL TABLES UPDATED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n  Base URL: https://airtable.com/{BASE_ID}")
    print(f"  Raw scraped: {raw}")
    print(f"  Filtered out: {total_filtered} ({n_co} excluded co, {n_lang} non-English, {n_test} test)")
    print(f"  Jobs loaded: {len(df)}")
    print(f"  Unknown: {unk} ({unk/len(df)*100:.1f}%)")
    print(f"  Companies: {df['Company'].nunique()}")
    print(f"  Roadmap mapped: {roadmap_hits}/{df['Company'].nunique()}")


if __name__ == "__main__":
    main()
