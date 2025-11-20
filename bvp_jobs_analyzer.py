import requests
import json
import time
from collections import Counter
import pandas as pd
import re

def infer_function_from_title(title):
    """Infer function/department from job title when not provided"""
    if not title:
        return "Unknown"
    
    title_lower = title.lower()
    
    # Engineering & Technical
    if any(word in title_lower for word in [
        'engineer', 'developer', 'software', 'sre', 'devops', 'architect', 
        'infrastructure', 'backend', 'frontend', 'fullstack', 'full stack',
        'mobile', 'ios', 'android', 'qa', 'sdet', 'technical program'
    ]):
        return "Engineering"
    
    # Sales & Business Development
    if any(word in title_lower for word in [
        'account executive', 'sales', 'business development', 'bdr', 'sdr',
        'account manager', 'account director', 'partnership manager',
        'sales development', 'revenue', 'commercial'
    ]):
        return "Sales"
    
    # Marketing
    if any(word in title_lower for word in [
        'marketing', 'growth marketing', 'demand gen', 'content', 'seo',
        'brand', 'campaigns', 'lifecycle marketing', 'product marketing'
    ]):
        return "Marketing"
    
    # Product Management
    if any(word in title_lower for word in [
        'product manager', 'product lead', 'product owner', 'pm ',
        'product operations', 'product strategy', 'product director'
    ]):
        return "Product"
    
    # Design
    if any(word in title_lower for word in [
        'designer', 'design', 'ux', 'ui', 'creative', 'visual'
    ]):
        return "Design"
    
    # Data & Analytics
    if any(word in title_lower for word in [
        'data scientist', 'data analyst', 'data engineer', 'analytics',
        'machine learning', 'ml engineer', 'ai researcher', 'data science'
    ]):
        return "Data & Analytics"
    
    # Customer Success & Support
    if any(word in title_lower for word in [
        'customer success', 'customer experience', 'customer support',
        'technical support', 'solutions engineer', 'implementation'
    ]):
        return "Customer Success"
    
    # Operations
    if any(word in title_lower for word in [
        'operations', 'ops ', 'office manager', 'business operations',
        'program manager', 'project manager'
    ]):
        return "Operations"
    
    # People/HR
    if any(word in title_lower for word in [
        'recruiter', 'recruiting', 'talent', 'people', 'hr ', 'human resources',
        'total rewards', 'people operations', 'people analytics'
    ]):
        return "People & Talent"
    
    # Finance & Accounting
    if any(word in title_lower for word in [
        'finance', 'accounting', 'accountant', 'controller', 'cfo',
        'financial', 'tax', 'audit', 'corporate development'
    ]):
        return "Finance"
    
    # Legal & Compliance
    if any(word in title_lower for word in [
        'legal', 'counsel', 'attorney', 'compliance', 'regulatory',
        'privacy', 'contracts'
    ]):
        return "Legal & Compliance"
    
    # IT & Systems
    if any(word in title_lower for word in [
        'it support', 'it engineer', 'systems admin', 'helpdesk',
        'desktop support'
    ]):
        return "IT"
    
    # Strategy & Business Development
    if any(word in title_lower for word in [
        'strategy', 'strategic', 'business development', 'partnerships',
        'corp dev', 'corporate development'
    ]):
        return "Strategy & Business Development"
    
    # Professional Services
    if any(word in title_lower for word in [
        'professional services', 'consulting', 'consultant', 'solutions architect'
    ]):
        return "Professional Services"
    
    return "Unknown"

def fetch_all_bvp_jobs():
    """Fetch all jobs from BVP job board using pagination"""
    url = "https://jobs.bvp.com/api-boards/search-jobs"
    all_jobs = []
    sequence = None
    page = 1
    
    while True:
        payload = {
            "board": {
                "id": "bessemer-ventures",
                "isParent": True
            },
            "grouped": False,
            "meta": {
                "size": 100
            },
            "query": {
                "promoteFeatured": True
            }
        }
        
        if sequence:
            payload["meta"]["sequence"] = sequence
        
        print(f"Fetching page {page}...")
        response = requests.post(url, json=payload)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break
        
        data = response.json()
        
        if "jobs" in data:
            jobs_data = data["jobs"]
            if isinstance(jobs_data, list):
                all_jobs.extend(jobs_data)
        
        print(f"  Fetched {len(all_jobs)} jobs so far (Total: {data.get('total', 'unknown')})")
        
        total = data.get('total', 0)
        if len(all_jobs) >= total:
            print(f"  Reached total! Stopping.")
            break
        
        if "meta" in data and "sequence" in data["meta"]:
            new_sequence = data["meta"]["sequence"]
            if new_sequence == sequence:
                print(f"  Sequence didn't change, stopping")
                break
            sequence = new_sequence
            page += 1
            time.sleep(0.5)
        else:
            print(f"  No sequence found, stopping")
            break
    
    return all_jobs

def analyze_jobs(jobs):
    """Analyze jobs by function and level"""
    
    functions = []
    levels = []
    titles = []
    inferred_count = 0
    
    for job in jobs:
        if not isinstance(job, dict):
            continue
            
        title = job.get("title", "")
        
        # Extract department - try API first, then infer from title
        departments = job.get("departments", [])
        if departments and isinstance(departments, list) and len(departments) > 0:
            department = departments[0]
        else:
            department = infer_function_from_title(title)
            if department != "Unknown":
                inferred_count += 1
        
        titles.append(title)
        functions.append(department)
        
        # Extract level from title
        title_lower = title.lower()
        if any(word in title_lower for word in ["vp", "vice president", "head of", "chief", "ceo", "cto", "cfo", "coo"]):
            level = "Executive"
        elif "director" in title_lower or ("lead" in title_lower and "lead generation" not in title_lower):
            level = "Director/Lead"
        elif any(word in title_lower for word in ["senior", "sr.", "sr ", "staff", "principal"]):
            level = "Senior"
        elif any(word in title_lower for word in ["junior", "jr.", "jr ", "associate", "entry"]):
            level = "Junior"
        else:
            level = "Mid-Level"
        
        levels.append(level)
    
    print(f"\n✨ Inferred function for {inferred_count} jobs from titles")
    
    function_counts = Counter(functions)
    level_counts = Counter(levels)
    
    report = {
        "total_jobs": len(jobs),
        "inferred_functions": inferred_count,
        "by_function": dict(function_counts.most_common()),
        "by_level": dict(level_counts.most_common()),
        "top_20_titles": Counter(titles).most_common(20)
    }
    
    return report, functions, levels, titles

def create_dataframe(jobs, functions, levels):
    """Create a pandas DataFrame for further analysis"""
    df_data = []
    
    for job, function, level in zip(jobs, functions, levels):
        if not isinstance(job, dict):
            continue
        
        company_name = job.get("companyName", "Unknown")
        
        locations = job.get("locations", [])
        if locations and isinstance(locations, list) and len(locations) > 0:
            location_name = locations[0]
        else:
            normalized_locs = job.get("normalizedLocations", [])
            if normalized_locs and isinstance(normalized_locs, list) and len(normalized_locs) > 0:
                if isinstance(normalized_locs[0], dict):
                    location_name = normalized_locs[0].get("label", "Unknown")
                else:
                    location_name = str(normalized_locs[0])
            else:
                location_name = "Unknown"
        
        url = job.get("url", "") or job.get("applyUrl", "")
        
        remote = job.get("remote", False)
        hybrid = job.get("hybrid", False)
        
        if remote:
            location_name = f"{location_name} (Remote)"
        elif hybrid:
            location_name = f"{location_name} (Hybrid)"
        
        df_data.append({
            "Title": job.get("title", ""),
            "Company": company_name,
            "Function": function,
            "Level": level,
            "Location": location_name,
            "Remote": "Yes" if remote else ("Hybrid" if hybrid else "No"),
            "URL": url
        })
    
    return pd.DataFrame(df_data)

if __name__ == "__main__":
    print("Starting BVP job board scraper...")
    print("=" * 60)

    jobs = fetch_all_bvp_jobs()

    print("\n" + "=" * 60)
    print(f"Successfully fetched {len(jobs)} jobs!")
    print("=" * 60)

    report, functions, levels, titles = analyze_jobs(jobs)

    print("\n📊 ANALYSIS RESULTS")
    print("=" * 60)
    print(f"\nTotal Jobs: {report['total_jobs']}")

    print("\n🏢 TOP 15 FUNCTIONS:")
    for func, count in list(report['by_function'].items())[:15]:
        percentage = (count / report['total_jobs']) * 100
        print(f"  {func:30s} {count:4d} ({percentage:5.1f}%)")

    print("\n📈 BY LEVEL:")
    for level, count in sorted(report['by_level'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / report['total_jobs']) * 100
        print(f"  {level:20s} {count:4d} ({percentage:5.1f}%)")

    print("\n🔥 TOP 20 JOB TITLES:")
    for title, count in report['top_20_titles']:
        print(f"  {count:3d}x {title}")

    df = create_dataframe(jobs, functions, levels)

    output_file = "bvp_jobs_analysis.csv"
    df.to_csv(output_file, index=False)
    print(f"\n💾 Data saved to: {output_file}")

    print("\n✅ Analysis complete!")
