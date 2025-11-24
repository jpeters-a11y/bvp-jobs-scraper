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
PERSONAL_ACCESS_TOKEN = 'patkRbMdDQXMQ11CZ.5eebdf0a940e5461a9926f84fe530478d2b367c5474534dc75a633a8ed4b6f32'
JOBS_TABLE = 'tblHHC9JcSHscBn6S'

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
    
    # NOTE: Korean departments (토스*) are NOT in this mapping
    # They will be inferred from job titles instead
    # Same for geographic/unclear departments - let title inference handle them
}

def normalize(func):
    # Already valid? Keep it
    if func in VALID:
        return func
    # Known mapping? Use it
    mapped = MAPPING.get(func)
    if mapped:
        return mapped
    # For everything else, try to infer from title
    return None  # Signal that we need to infer from title

print("Normalizing functions...")
df = pd.read_csv('bvp_jobs_analysis.csv')
print(f"CSV has {len(df[df['Function'] == 'Unknown'])} Unknown")

# First pass: use mapping
df['Fixed'] = df['Function'].apply(normalize)

# Second pass: infer from title for unmapped values
needs_inference = df['Fixed'].isna()
print(f"Inferring from title for {needs_inference.sum()} unmapped departments...")
df.loc[needs_inference, 'Fixed'] = df.loc[needs_inference, 'Title'].apply(infer_function_from_title)
print(f"After mapping: {len(df[df['Fixed'] == 'Unknown'])} Unknown ({len(df[df['Fixed'] == 'Unknown'])/len(df)*100:.1f}%)")

unmapped = df[(df['Function'] != 'Unknown') & (df['Fixed'] == 'Unknown')]
if len(unmapped) > 0:
    print(f"\nUnmapped ({len(unmapped)} jobs):")
    print(unmapped['Function'].value_counts())
else:
    print("\n✅ All non-Unknown values successfully mapped!")

print("\n" + "="*60)
input("Press Enter to upload to Airtable (Ctrl+C to cancel)...")

api = Api(PERSONAL_ACCESS_TOKEN)
jobs_table = api.table(BASE_ID, JOBS_TABLE)

print("\nClearing existing records...")
existing = jobs_table.all()
if existing:
    for i in range(0, len(existing), 10):
        jobs_table.batch_delete([r['id'] for r in existing[i:i+10]])
        time.sleep(0.2)

print(f"Uploading {len(df)} jobs...")
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
    if (i // 10 + 1) % 100 == 0:
        print(f"  Batch {i // 10 + 1}/585")
    time.sleep(0.2)

print("\n✅ Upload complete!")
print(f"Airtable now has {len(df[df['Fixed'] == 'Unknown'])} Unknown records")
