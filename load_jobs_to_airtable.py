import pandas as pd
from datetime import datetime
import time
from collections import Counter
import os

from pyairtable import Api

BASE_ID = 'appKRyK4KfiGX9ojv'
PERSONAL_ACCESS_TOKEN = os.environ.get('AIRTABLE_TOKEN', 'patkRbMdDQXMQ11CZ.5eebdf0a940e5461a9926f84fe530478d2b367c5474534dc75a633a8ed4b6f32')

JOBS_TABLE = 'tblHHC9JcSHscBn6S'
COMPANY_ANALYTICS_TABLE = 'tbl1DjAksyFMLl5XZ'
FUNCTION_ANALYTICS_TABLE = 'tbl3ofnnggrF39E2A'
WEEKLY_SNAPSHOTS_TABLE = 'tbl24qL4zM3cZ9QDr'
TALENT_POOLING_TABLE = 'tblnIBdiR7P5mmUn7'

FUNCTION_MAPPING = {
    'Engineering': 'Engineering', 'Sales': 'Sales', 'Marketing': 'Marketing',
    'Product': 'Product', 'Design': 'Design', 'Data & Analytics': 'Data & Analytics',
    'Customer Success': 'Customer Success', 'Operations': 'Operations',
    'People & Talent': 'People & Talent', 'Finance': 'Finance',
    'Legal & Compliance': 'Legal & Compliance', 'IT': 'IT',
    'Strategy & Business Development': 'Strategy & Business Development',
    'Professional Services': 'Professional Services', 'Unknown': 'Unknown',
    'Product & Design': 'Product', 'Product Management': 'Product',
    'Product Development': 'Product', 'Product Development ': 'Product',
    'Product Management, Support, & Operations': 'Product',
    'Commercial': 'Sales', 'Go-To-Market': 'Sales', 'Sales SMB': 'Sales',
    'Account Executives': 'Sales', 'APAC': 'Sales',
    'Marketing Operations': 'Marketing', 'Growth': 'Marketing', 'Marketing ': 'Marketing',
    'Development': 'Engineering', 'AI Research & Engineering': 'Engineering',
    'Technical Program Management': 'Engineering', 'Technical Program Management ': 'Engineering',
    'Data': 'Data & Analytics', 'Ops Analytics': 'Data & Analytics',
    'Clinical Operations': 'Operations', 'Supply Success': 'Operations', 'Precision': 'Operations',
    'Client Impact': 'Customer Success', 'Customer Experience': 'Customer Success',
    'Customer Success & Solutions': 'Customer Success',
    'People Operations': 'People & Talent', 'Talent': 'People & Talent',
    'Strategy & BD': 'Strategy & Business Development', 'Partnerships': 'Strategy & Business Development',
    'Technology': 'IT', 'Compliance': 'Legal & Compliance',
    'Global PSF': 'Professional Services', 'Crete Professionals Alliance': 'Unknown',
}

def infer_function_from_title(title):
    if not title:
        return "Unknown"
    t = title.lower()
    if any(w in t for w in ['fp&a', 'fpa', 'finance', 'accounting', 'accountant', 'controller', 'cfo', 'financial', 'tax', 'audit', 'payroll', 'bookkeeper', 'accounts payable', 'accounts receivable', 'treasury']):
        return "Finance"
    if any(w in t for w in ['legal', 'counsel', 'attorney', 'compliance', 'regulatory', 'privacy', 'contracts', 'grc', 'governance']):
        return "Legal & Compliance"
    if any(w in t for w in ['recruiter', 'recruiting', 'talent acquisition', 'talent', 'people business partner', 'people partner', 'people enablement', 'hr ', 'hrbp', 'human resources', 'total rewards', 'people operations', 'people analytics', 'people experience']):
        return "People & Talent"
    if any(w in t for w in ['engineer', 'developer', 'software', 'sre', 'devops', 'architect', 'infrastructure', 'backend', 'frontend', 'fullstack', 'full stack', 'mobile', 'ios', 'android', 'qa', 'sdet', 'technical program', 'firmware', 'embedded', 'hardware engineer', 'test engineer']):
        return "Engineering"
    if any(w in t for w in ['account executive', 'sales', 'business development', 'bdr', 'sdr', 'account manager', 'account director', 'partnership manager', 'sales development', 'revenue', 'commercial', 'inside sales', 'enterprise sales', 'gtm manager', 'relationship manager', 'key account']):
        return "Sales"
    if any(w in t for w in ['marketing', 'growth marketing', 'demand gen', 'content', 'seo', 'brand', 'campaigns', 'lifecycle marketing', 'product marketing', 'growth marketer', 'social media', 'communications', 'public affairs', 'community manager']):
        return "Marketing"
    if any(w in t for w in ['product manager', 'product lead', 'product owner', 'product director', 'product analyst', 'product designer']):
        if 'support' not in t and 'operations' not in t:
            return "Product"
    if any(w in t for w in ['designer', 'design', 'ux', 'ui', 'creative', 'visual']):
        return "Design"
    if any(w in t for w in ['data scientist', 'data analyst', 'data engineer', 'analytics', 'machine learning', 'ml engineer', 'ai researcher', 'data science', 'data specialist', 'data platform']):
        return "Data & Analytics"
    if any(w in t for w in ['customer success', 'customer experience', 'customer support', 'technical support', 'implementation manager', 'customer care', 'customer architect', 'customer education', 'support specialist', 'support engineer', 'customer strategy']):
        return "Customer Success"
    if any(w in t for w in ['it support', 'it engineer', 'it administrator', 'it specialist', 'systems admin', 'helpdesk', 'desktop support', 'salesforce admin', 'it governance', 'it planning', 'it cloud', 'service desk']):
        return "IT"
    if any(w in t for w in ['strategy', 'strategic', 'business development', 'partnerships', 'corp dev', 'corporate development', 'chief of staff']):
        return "Strategy & Business Development"
    if any(w in t for w in ['operations', 'ops manager', 'ops specialist', 'ops generalist', 'office manager', 'business operations', 'program manager', 'project manager', 'executive assistant', 'admin', 'procurement', 'process', 'implementation specialist', 'solutions operations', 'gtm operations', 'product operations', 'fraud analyst']):
        return "Operations"
    if any(w in t for w in ['professional services', 'consulting', 'consultant', 'solutions architect']):
        return "Professional Services"
    return "Unknown"

def normalize_function(func, title):
    mapped = FUNCTION_MAPPING.get(func)
    if mapped:
        return mapped
    return infer_function_from_title(title)

def load_jobs_to_airtable(csv_path, api):
    print("Loading CSV data...")
    df = pd.read_csv(csv_path)
    df['Function_Normalized'] = df.apply(lambda row: normalize_function(row['Function'], row['Title']), axis=1)
    
    print(f"Found {len(df)} jobs to upload")
    jobs_table = api.table(BASE_ID, JOBS_TABLE)
    
    print("Clearing existing records...")
    existing = jobs_table.all()
    if existing:
        for i in range(0, len(existing), 10):
            jobs_table.batch_delete([r['id'] for r in existing[i:i+10]])
            time.sleep(0.2)
        print(f"  Deleted {len(existing)} existing records")
    
    print(f"Uploading in {(len(df) + 9) // 10} batches...")
    for i in range(0, len(df), 10):
        batch = df.iloc[i:i+10]
        records = [{
            'Job Title': str(row['Title']),
            'Company': str(row['Company']),
            'Function': str(row['Function_Normalized']),
            'Level': str(row['Level']),
            'Location': str(row['Location']),
            'Remote': str(row['Remote']),
            'URL': str(row['URL']) if pd.notna(row['URL']) else '',
            'Last Updated': datetime.now().isoformat()
        } for _, row in batch.iterrows()]
        jobs_table.batch_create(records)
        if (i // 10 + 1) % 50 == 0:
            print(f"  Uploaded batch {i // 10 + 1}/{(len(df) + 9) // 10}")
        time.sleep(0.2)
    
    print("✅ All jobs uploaded!")
    return df

def clear_and_create_function_analytics(df, api):
    print("\nCreating function analytics...")
    table = api.table(BASE_ID, FUNCTION_ANALYTICS_TABLE)
    existing = table.all()
    if existing:
        for i in range(0, len(existing), 10):
            table.batch_delete([r['id'] for r in existing[i:i+10]])
            time.sleep(0.2)
    
    stats = df.groupby('Function_Normalized').agg({
        'Title': 'count',
        'Company': lambda x: x.nunique(),
        'Remote': lambda x: (x == 'Yes').sum()
    }).reset_index()
    stats.columns = ['Function', 'Total', 'Companies', 'Remote']
    stats['Pct'] = stats['Total'] / len(df)
    stats['Remote_Pct'] = stats['Remote'] / stats['Total']
    
    for func in stats['Function']:
        func_df = df[df['Function_Normalized'] == func]
        stats.loc[stats['Function'] == func, 'Executive'] = len(func_df[func_df['Level'] == 'Executive'])
        stats.loc[stats['Function'] == func, 'Senior'] = len(func_df[func_df['Level'] == 'Senior'])
    
    records = [{'Function': str(row['Function']), 'Total Jobs': int(row['Total']),
                'Percentage of Total': float(row['Pct']), 'Remote Jobs': int(row['Remote']),
                'Remote Percentage': float(row['Remote_Pct']), 'Companies Hiring': int(row['Companies']),
                'Executive Roles': int(row['Executive']), 'Senior Roles': int(row['Senior']),
                'Last Updated': datetime.now().isoformat()} for _, row in stats.iterrows()]
    table.batch_create(records)
    print(f"✅ Created {len(records)} function analytics records")

def clear_and_create_company_analytics(df, api):
    print("\nCreating company analytics...")
    table = api.table(BASE_ID, COMPANY_ANALYTICS_TABLE)
    existing = table.all()
    if existing:
        for i in range(0, len(existing), 10):
            table.batch_delete([r['id'] for r in existing[i:i+10]])
            time.sleep(0.2)
    
    stats = df.groupby('Company').agg({
        'Title': 'count',
        'Function_Normalized': lambda x: x.nunique(),
        'Remote': lambda x: (x == 'Yes').sum() / len(x) if len(x) > 0 else 0
    }).reset_index()
    stats.columns = ['Company', 'Total', 'Functions', 'Remote_Pct']
    
    for company in stats['Company']:
        comp_df = df[df['Company'] == company]
        stats.loc[stats['Company'] == company, 'Engineering'] = len(comp_df[comp_df['Function_Normalized'] == 'Engineering'])
        stats.loc[stats['Company'] == company, 'Sales'] = len(comp_df[comp_df['Function_Normalized'] == 'Sales'])
        stats.loc[stats['Company'] == company, 'Marketing'] = len(comp_df[comp_df['Function_Normalized'] == 'Marketing'])
        stats.loc[stats['Company'] == company, 'Product'] = len(comp_df[comp_df['Function_Normalized'] == 'Product'])
    
    stats = stats.sort_values('Total', ascending=False).head(50)
    
    for i in range(0, len(stats), 10):
        batch = stats.iloc[i:i+10]
        records = [{'Company Name': str(row['Company']), 'Total Jobs': int(row['Total']),
                    'Engineering Jobs': int(row['Engineering']), 'Sales Jobs': int(row['Sales']),
                    'Marketing Jobs': int(row['Marketing']), 'Product Jobs': int(row['Product']),
                    'Remote Percentage': float(row['Remote_Pct']), 'Unique Functions': int(row['Functions']),
                    'Last Updated': datetime.now().isoformat()} for _, row in batch.iterrows()]
        table.batch_create(records)
        time.sleep(0.2)
    
    print(f"✅ Created {len(stats)} company analytics records")

def create_weekly_snapshot(df, api):
    print("\nCreating weekly snapshot...")
    table = api.table(BASE_ID, WEEKLY_SNAPSHOTS_TABLE)
    
    snapshot = {
        'Snapshot Date': datetime.now().strftime('%Y-%m-%d'),
        'Total Jobs': len(df),
        'Total Companies Hiring': df['Company'].nunique(),
        'Engineering Jobs': len(df[df['Function_Normalized'] == 'Engineering']),
        'Sales Jobs': len(df[df['Function_Normalized'] == 'Sales']),
        'Marketing Jobs': len(df[df['Function_Normalized'] == 'Marketing']),
        'Product Jobs': len(df[df['Function_Normalized'] == 'Product']),
        'Remote Jobs': len(df[df['Remote'] == 'Yes']),
        'Remote Percentage': len(df[df['Remote'] == 'Yes']) / len(df),
        'Notes': 'Automated weekly update'
    }
    table.create(snapshot)
    print("✅ Created weekly snapshot")

def clear_and_create_talent_pooling(df, api):
    print("\nIdentifying talent pooling opportunities...")
    table = api.table(BASE_ID, TALENT_POOLING_TABLE)
    existing = table.all()
    if existing:
        for i in range(0, len(existing), 10):
            table.batch_delete([r['id'] for r in existing[i:i+10]])
            time.sleep(0.2)
    
    opportunities = []
    for title, group in df.groupby('Title'):
        companies = list(set(group['Company']))
        if len(companies) >= 2:
            opportunities.append({
                'Title': title, 'Num_Companies': len(companies),
                'Total_Openings': len(group), 'Companies': companies,
                'Function': group['Function_Normalized'].mode()[0] if len(group) > 0 else 'Unknown',
                'Level': group['Level'].mode()[0] if len(group) > 0 else 'Unknown'
            })
    
    opportunities = sorted(opportunities, key=lambda x: x['Num_Companies'], reverse=True)[:30]
    
    for opp in opportunities:
        companies_list = ', '.join(opp['Companies'][:10])
        if len(opp['Companies']) > 10:
            companies_list += f" (+ {len(opp['Companies']) - 10} more)"
        priority = 'High (5+ companies)' if opp['Num_Companies'] >= 5 else ('Medium (3-4 companies)' if opp['Num_Companies'] >= 3 else 'Low (2 companies)')
        table.create({'Job Title': str(opp['Title']), 'Number of Companies': int(opp['Num_Companies']),
                      'Total Openings': int(opp['Total_Openings']), 'Companies': companies_list,
                      'Function': str(opp['Function']), 'Level': str(opp['Level']),
                      'Priority': priority, 'Last Updated': datetime.now().isoformat()})
        time.sleep(0.1)
    print(f"✅ Created {len(opportunities)} talent pooling opportunities")

def main():
    print("=" * 60)
    print("BVP PORTFOLIO JOBS - AIRTABLE INTEGRATION")
    print("=" * 60 + "\n")
    
    if PERSONAL_ACCESS_TOKEN == 'YOUR_AIRTABLE_PERSONAL_ACCESS_TOKEN':
        print("❌ ERROR: Set the AIRTABLE_TOKEN environment variable\n")
        return
    
    api = Api(PERSONAL_ACCESS_TOKEN)
    csv_path = 'bvp_jobs_analysis.csv'
    df = load_jobs_to_airtable(csv_path, api)
    clear_and_create_function_analytics(df, api)
    clear_and_create_company_analytics(df, api)
    create_weekly_snapshot(df, api)
    clear_and_create_talent_pooling(df, api)
    
    print(f"\n{'=' * 60}\n✅ ALL DATA LOADED TO AIRTABLE!\n{'=' * 60}")
    print(f"\nBase URL: https://airtable.com/{BASE_ID}\n")

if __name__ == "__main__":
    main()
