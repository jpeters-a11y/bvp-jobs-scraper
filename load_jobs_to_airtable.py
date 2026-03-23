import pandas as pd
from datetime import datetime
import time
from collections import Counter

# YOU'LL NEED TO INSTALL: pip install pyairtable
from pyairtable import Api

# Airtable configuration  
BASE_ID = 'appKRyK4KfiGX9ojv'
PERSONAL_ACCESS_TOKEN = 'patkRbMdDQXMQ11CZ.5eebdf0a940e5461a9926f84fe530478d2b367c5474534dc75a633a8ed4b6f32'  # You'll need to create this

# Table IDs from the creation
JOBS_TABLE = 'tblHHC9JcSHscBn6S'
COMPANY_ANALYTICS_TABLE = 'tbl1DjAksyFMLl5XZ'
FUNCTION_ANALYTICS_TABLE = 'tbl3ofnnggrF39E2A'
WEEKLY_SNAPSHOTS_TABLE = 'tbl24qL4zM3cZ9QDr'
TALENT_POOLING_TABLE = 'tblnIBdiR7P5mmUn7'

# Mapping of all function values to Airtable's predefined categories
FUNCTION_MAPPING = {
    'Engineering': 'Engineering',
    'Sales': 'Sales',
    'Marketing': 'Marketing',
    'Product': 'Product',
    'Design': 'Design',
    'Data & Analytics': 'Data & Analytics',
    'Customer Success': 'Customer Success',
    'Operations': 'Operations',
    'People & Talent': 'People & Talent',
    'Finance': 'Finance',
    'Legal & Compliance': 'Legal & Compliance',
    'IT': 'IT',
    'Strategy & Business Development': 'Strategy & Business Development',
    'Professional Services': 'Professional Services',
    
    # Map variations to main categories
    'Product & Design': 'Product',
    'Product Management': 'Product',
    'Product Development': 'Product',
    'Strategy & BD': 'Strategy & Business Development',
    'Commercial': 'Sales',
    'Go-To-Market': 'Sales',
    'Sales SMB': 'Sales',
    'Account Executives': 'Sales',
    'Marketing Operations': 'Marketing',
    'Growth': 'Marketing',
    'Data': 'Data & Analytics',
    'Clinical Operations': 'Operations',
    'Partnerships': 'Strategy & Business Development',
    'People Operations': 'People & Talent',
    'Technology': 'IT',
    'Compliance': 'Legal & Compliance',
    'Talent': 'People & Talent',
    'Client Impact': 'Customer Success',
    'Supply Success': 'Operations',
    'Precision': 'Operations',
    'Ops Analytics': 'Data & Analytics',
    'Development': 'Engineering',
    'Product Development ': 'Product',
    'AI Research & Engineering': 'Engineering',
    'Product Management, Support, & Operations': 'Product',
    'Technical Program Management ': 'Engineering',
    'Technical Program Management': 'Engineering',
    'Customer Experience': 'Customer Success',
    'Customer Success & Solutions': 'Customer Success',
    'Marketing ': 'Marketing',
    'APAC': 'Sales',
    'Global PSF': 'Professional Services',
    
    # Everything else
    'Unknown': 'Unknown'
}

def normalize_function(func):
    """Map function to one of the predefined Airtable categories"""
    return FUNCTION_MAPPING.get(func, 'Unknown')

def load_jobs_to_airtable(csv_path, api):
    """Load all jobs from CSV into Airtable Jobs table"""
    print("Loading CSV data...")
    df = pd.read_csv(csv_path)
    
    # Normalize function values
    df['Function_Normalized'] = df['Function'].apply(normalize_function)
    
    print(f"Found {len(df)} jobs to upload")
    
    # Get the table
    jobs_table = api.table(BASE_ID, JOBS_TABLE)
    
    # Airtable has a limit of 10 records per batch
    batch_size = 10
    total_batches = (len(df) + batch_size - 1) // batch_size
    
    print(f"Uploading in {total_batches} batches...")
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        records = []
        
        for _, row in batch.iterrows():
            record = {
                'Job Title': str(row['Title']),
                'Company': str(row['Company']),
                'Function': str(row['Function_Normalized']),  # Use normalized function
                'Level': str(row['Level']),
                'Location': str(row['Location']),
                'Remote': str(row['Remote']),
                'URL': str(row['URL']) if pd.notna(row['URL']) else '',
                'Last Updated': datetime.now().isoformat()
            }
            records.append(record)
        
        # Create records in batch
        jobs_table.batch_create(records)
        
        batch_num = (i // batch_size) + 1
        if batch_num % 50 == 0:
            print(f"  Uploaded batch {batch_num}/{total_batches}")
        
        # Rate limiting - be nice to Airtable API
        time.sleep(0.2)
    
    print("✅ All jobs uploaded!")
    return df

def create_function_analytics(df, api):
    """Create function analytics summary"""
    print("\nCreating function analytics...")
    
    table = api.table(BASE_ID, FUNCTION_ANALYTICS_TABLE)
    
    # Use normalized function
    df['Function_Normalized'] = df['Function'].apply(normalize_function)
    
    # Calculate stats by function
    function_stats = df.groupby('Function_Normalized').agg({
        'Title': 'count',
        'Company': lambda x: x.nunique(),
        'Remote': lambda x: (x == 'Yes').sum()
    }).reset_index()
    
    function_stats.columns = ['Function', 'Total', 'Companies', 'Remote']
    
    # Add percentages
    total_jobs = len(df)
    function_stats['Pct'] = function_stats['Total'] / total_jobs
    function_stats['Remote_Pct'] = function_stats['Remote'] / function_stats['Total']
    
    # Add level breakdowns
    for func in function_stats['Function']:
        func_df = df[df['Function_Normalized'] == func]
        function_stats.loc[function_stats['Function'] == func, 'Executive'] = \
            len(func_df[func_df['Level'] == 'Executive'])
        function_stats.loc[function_stats['Function'] == func, 'Senior'] = \
            len(func_df[func_df['Level'] == 'Senior'])
    
    # Upload to Airtable
    records = []
    for _, row in function_stats.iterrows():
        record = {
            'Function': str(row['Function']),
            'Total Jobs': int(row['Total']),
            'Percentage of Total': float(row['Pct']),
            'Remote Jobs': int(row['Remote']),
            'Remote Percentage': float(row['Remote_Pct']),
            'Companies Hiring': int(row['Companies']),
            'Executive Roles': int(row['Executive']),
            'Senior Roles': int(row['Senior']),
            'Last Updated': datetime.now().isoformat()
        }
        records.append(record)
    
    table.batch_create(records)
    print(f"✅ Created {len(records)} function analytics records")

def create_company_analytics(df, api):
    """Create company analytics summary"""
    print("\nCreating company analytics...")
    
    table = api.table(BASE_ID, COMPANY_ANALYTICS_TABLE)
    
    # Use normalized function
    df['Function_Normalized'] = df['Function'].apply(normalize_function)
    
    # Calculate stats by company
    company_stats = df.groupby('Company').agg({
        'Title': 'count',
        'Function_Normalized': lambda x: x.nunique(),
        'Remote': lambda x: (x == 'Yes').sum() / len(x) if len(x) > 0 else 0
    }).reset_index()
    
    company_stats.columns = ['Company', 'Total', 'Functions', 'Remote_Pct']
    
    # Add function-specific counts
    for company in company_stats['Company']:
        comp_df = df[df['Company'] == company]
        company_stats.loc[company_stats['Company'] == company, 'Engineering'] = \
            len(comp_df[comp_df['Function_Normalized'] == 'Engineering'])
        company_stats.loc[company_stats['Company'] == company, 'Sales'] = \
            len(comp_df[comp_df['Function_Normalized'] == 'Sales'])
        company_stats.loc[company_stats['Company'] == company, 'Marketing'] = \
            len(comp_df[comp_df['Function_Normalized'] == 'Marketing'])
        company_stats.loc[company_stats['Company'] == company, 'Product'] = \
            len(comp_df[comp_df['Function_Normalized'] == 'Product'])
    
    # Sort by total jobs and take top 50
    company_stats = company_stats.sort_values('Total', ascending=False).head(50)
    
    # Upload to Airtable in batches
    batch_size = 10
    for i in range(0, len(company_stats), batch_size):
        batch = company_stats.iloc[i:i+batch_size]
        records = []
        
        for _, row in batch.iterrows():
            record = {
                'Company Name': str(row['Company']),
                'Total Jobs': int(row['Total']),
                'Engineering Jobs': int(row['Engineering']),
                'Sales Jobs': int(row['Sales']),
                'Marketing Jobs': int(row['Marketing']),
                'Product Jobs': int(row['Product']),
                'Remote Percentage': float(row['Remote_Pct']),
                'Unique Functions': int(row['Functions']),
                'Last Updated': datetime.now().isoformat()
            }
            records.append(record)
        
        table.batch_create(records)
        time.sleep(0.2)
    
    print(f"✅ Created {len(company_stats)} company analytics records")

def create_weekly_snapshot(df, api):
    """Create a snapshot of current state"""
    print("\nCreating weekly snapshot...")
    
    table = api.table(BASE_ID, WEEKLY_SNAPSHOTS_TABLE)
    
    # Use normalized function
    df['Function_Normalized'] = df['Function'].apply(normalize_function)
    
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
        'Notes': 'Initial data load'
    }
    
    table.create(snapshot)
    print("✅ Created weekly snapshot")

def create_talent_pooling_opportunities(df, api):
    """Identify roles open at multiple companies"""
    print("\nIdentifying talent pooling opportunities...")
    
    table = api.table(BASE_ID, TALENT_POOLING_TABLE)
    
    # Use normalized function
    df['Function_Normalized'] = df['Function'].apply(normalize_function)
    
    # Count jobs by title - simpler approach
    title_groups = df.groupby('Title')
    
    opportunities = []
    for title, group in title_groups:
        companies = list(set(group['Company']))
        num_companies = len(companies)
        
        # Only include if at 2+ companies
        if num_companies >= 2:
            # Get most common function and level for this title
            function = group['Function_Normalized'].mode()[0] if len(group) > 0 else 'Unknown'
            level = group['Level'].mode()[0] if len(group) > 0 else 'Unknown'
            
            opportunities.append({
                'Title': title,
                'Num_Companies': num_companies,
                'Total_Openings': len(group),
                'Companies': companies,
                'Function': function,
                'Level': level
            })
    
    # Sort by number of companies and take top 30
    opportunities = sorted(opportunities, key=lambda x: x['Num_Companies'], reverse=True)[:30]
    
    # Upload to Airtable
    for opp in opportunities:
        companies_list = ', '.join(opp['Companies'][:10])  # Limit to first 10 companies
        if len(opp['Companies']) > 10:
            companies_list += f" (+ {len(opp['Companies']) - 10} more)"
        
        # Determine priority
        if opp['Num_Companies'] >= 5:
            priority = 'High (5+ companies)'
        elif opp['Num_Companies'] >= 3:
            priority = 'Medium (3-4 companies)'
        else:
            priority = 'Low (2 companies)'
        
        record = {
            'Job Title': str(opp['Title']),
            'Number of Companies': int(opp['Num_Companies']),
            'Total Openings': int(opp['Total_Openings']),
            'Companies': companies_list,
            'Function': str(opp['Function']),
            'Level': str(opp['Level']),
            'Priority': priority,
            'Last Updated': datetime.now().isoformat()
        }
        
        table.create(record)
        time.sleep(0.1)
    
    print(f"✅ Created {len(opportunities)} talent pooling opportunities")

def main():
    print("=" * 60)
    print("BVP PORTFOLIO JOBS - AIRTABLE INTEGRATION")
    print("=" * 60)
    print()
    
    # Check for token
    if PERSONAL_ACCESS_TOKEN == 'YOUR_AIRTABLE_PERSONAL_ACCESS_TOKEN':
        print("❌ ERROR: You need to set your Airtable Personal Access Token")
        print()
        print("To create one:")
        print("1. Go to https://airtable.com/create/tokens")
        print("2. Click 'Create new token'")
        print("3. Give it a name (e.g., 'BVP Jobs Integration')")
        print("4. Add scopes: data.records:read, data.records:write")
        print("5. Add access to base: Portfolio Jobs Intelligence")
        print("6. Copy the token and paste it in this script")
        print()
        return
    
    # Initialize API
    api = Api(PERSONAL_ACCESS_TOKEN)
    
    # Load CSV
    csv_path = 'bvp_jobs_analysis.csv'
    df = load_jobs_to_airtable(csv_path, api)
    
    # Create analytics
    create_function_analytics(df, api)
    create_company_analytics(df, api)
    create_weekly_snapshot(df, api)
    create_talent_pooling_opportunities(df, api)
    
    print()
    print("=" * 60)
    print("✅ ALL DATA LOADED TO AIRTABLE!")
    print("=" * 60)
    print()
    print(f"Base URL: https://airtable.com/{BASE_ID}")
    print()
    print("Tables created:")
    print("  • Jobs - All 5800 job listings")
    print("  • Function Analytics - Summary by function")
    print("  • Company Analytics - Top 50 companies by job count")
    print("  • Weekly Snapshots - Trend tracking")
    print("  • Talent Pooling Opportunities - Multi-company roles")
    print()

if __name__ == "__main__":
    main()
