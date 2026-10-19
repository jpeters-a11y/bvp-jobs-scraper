import re
import sys
import json
import time
from collections import Counter

import pandas as pd
import requests

try:
    import truststore; truststore.inject_into_ssl()   # corporate proxy fix (local only)
except ImportError:
    pass

BASE = "https://jobs.bvp.com"
API = f"{BASE}/api-boards/search-jobs"
BOARD = {"id": "bessemer-ventures", "isParent": True}
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
CSRF_PATTERNS = [
    r'<meta[^>]+name="csrf-token"[^>]+content="([^"]+)"',
    r'"csrfToken"\s*:\s*"([^"]+)"',
    r'"_csrf"\s*:\s*"([^"]+)"',
    r'csrf[_-]?token["\']?\s*[:=]\s*["\']([A-Za-z0-9_\-]{20,})',
]


def _new_session():
    """Load the board page to get a session cookie + CSRF token (Consider requires both)."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    html = s.get(f"{BASE}/jobs", timeout=30).text
    token = next((m.group(1) for p in CSRF_PATTERNS if (m := re.search(p, html, re.I))), None)
    if not token:
        sys.exit("FATAL: CSRF token not found on jobs.bvp.com; page structure changed")
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": BASE,
        "Referer": f"{BASE}/jobs",
        "x-csrf-token": token,
    })
    return s

def infer_function_from_title(title):
    """Infer function/department from job title when not provided"""
    if not title:
        return "Unknown"
    
    title_lower = title.lower()
    
    # Finance & Accounting (check early to avoid conflicts with "operations")
    if any(word in title_lower for word in [
        'fp&a', 'fpa', 'finance', 'accounting', 'accountant', 'controller', 'cfo',
        'financial', 'tax', 'audit', 'payroll', 'bookkeeper', 'accounts payable',
        'accounts receivable', 'treasury'
    ]):
        return "Finance"
    
    # Legal & Compliance (check early)
    if any(word in title_lower for word in [
        'legal', 'counsel', 'attorney', 'compliance', 'regulatory',
        'privacy', 'contracts', 'grc', 'governance'
    ]):
        return "Legal & Compliance"
    
    # People/HR (check before Operations to catch "People Operations")
    if any(word in title_lower for word in [
        'recruiter', 'recruiting', 'talent acquisition', 'talent', 'people business partner',
        'people partner', 'people enablement', 'hr ', 'hrbp', 'human resources',
        'total rewards', 'people operations', 'people analytics', 'people experience'
    ]):
        return "People & Talent"
    
    # Engineering & Technical
    if any(word in title_lower for word in [
        'engineer', 'developer', 'software', 'sre', 'devops', 'architect', 
        'infrastructure', 'backend', 'frontend', 'fullstack', 'full stack',
        'mobile', 'ios', 'android', 'qa', 'sdet', 'technical program', 'firmware',
        'embedded', 'hardware engineer', 'test engineer'
    ]):
        return "Engineering"
    
    # Sales & Business Development
    if any(word in title_lower for word in [
        'account executive', 'sales', 'business development', 'bdr', 'sdr',
        'account manager', 'account director', 'partnership manager',
        'sales development', 'revenue', 'commercial', 'inside sales',
        'enterprise sales', 'gtm manager', 'relationship manager', 'key account'
    ]):
        return "Sales"
    
    # Marketing (check before Operations)
    if any(word in title_lower for word in [
        'marketing', 'growth marketing', 'demand gen', 'content', 'seo',
        'brand', 'campaigns', 'lifecycle marketing', 'product marketing',
        'growth marketer', 'social media', 'communications', 'public affairs',
        'community manager'
    ]):
        return "Marketing"
    
    # Product Management
    if any(word in title_lower for word in [
        'product manager', 'product lead', 'product owner',
        'product director', 'product analyst', 'product designer'
    ]):
        # Exclude "product support" and "product operations"
        if 'support' in title_lower or 'operations' in title_lower:
            pass  # Will be caught by other categories
        else:
            return "Product"
    
    # Design
    if any(word in title_lower for word in [
        'designer', 'design', 'ux', 'ui', 'creative', 'visual'
    ]):
        return "Design"
    
    # Data & Analytics
    if any(word in title_lower for word in [
        'data scientist', 'data analyst', 'data engineer', 'analytics',
        'machine learning', 'ml engineer', 'ai researcher', 'data science',
        'data specialist', 'data platform'
    ]):
        return "Data & Analytics"
    
    # Customer Success & Support (check before Operations)
    if any(word in title_lower for word in [
        'customer success', 'customer experience', 'customer support',
        'technical support', 'implementation manager', 'customer care',
        'customer architect', 'customer education', 'support specialist',
        'support engineer', 'customer strategy'
    ]):
        return "Customer Success"
    
    # IT & Systems
    if any(word in title_lower for word in [
        'it support', 'it engineer', 'it administrator', 'it specialist',
        'systems admin', 'helpdesk', 'desktop support', 'salesforce admin',
        'it governance', 'it planning', 'it cloud', 'service desk'
    ]):
        return "IT"
    
    # Strategy & Business Development (check before Operations)
    if any(word in title_lower for word in [
        'strategy', 'strategic', 'business development', 'partnerships',
        'corp dev', 'corporate development', 'chief of staff'
    ]):
        return "Strategy & Business Development"
    
    # Operations (check last as it's broad)
    if any(word in title_lower for word in [
        'operations', 'ops manager', 'ops specialist', 'ops generalist',
        'office manager', 'business operations', 'program manager',
        'project manager', 'executive assistant', 'admin', 'procurement',
        'process', 'implementation specialist', 'solutions operations',
        'gtm operations', 'product operations', 'fraud analyst'
    ]):
        return "Operations"
    
    # Professional Services
    if any(word in title_lower for word in [
        'professional services', 'consulting', 'consultant', 'solutions architect'
    ]):
        return "Professional Services"
    
    return "Unknown"

def fetch_all_bvp_jobs(page_size=100, max_retries=3):
    s, jobs, seq, total = _new_session(), [], None, None
    while True:
        payload = {"meta": {"size": page_size}, "board": BOARD,
                   "query": {"promoteFeatured": True}, "grouped": False}
        if seq:
            payload["meta"]["sequence"] = seq
        for attempt in range(max_retries):
            try:
                r = s.post(API, json=payload, timeout=30)
                if r.status_code in (403, 412):        # token/session expired → refresh
                    s = _new_session(); continue
                r.raise_for_status()
                data = r.json(); break
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    sys.exit(f"FATAL: request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)
        else:
            sys.exit("FATAL: repeated 412/403 even after session refresh")

        total = data.get("total", total)
        for item in data.get("jobs", []):
            if isinstance(item, dict) and "jobs" in item:
                jobs.extend(item["jobs"])
            elif isinstance(item, dict):
                jobs.append(item)
        print(f"  {len(jobs)}/{total}")
        seq = (data.get("meta") or {}).get("sequence")
        if not seq or (total and len(jobs) >= total):
            break
        time.sleep(1)

    if not jobs or (total and len(jobs) < 0.8 * total):
        sys.exit(f"FATAL: fetched {len(jobs)} of {total}; refusing to overwrite Airtable")
    return jobs

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
