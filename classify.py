"""v2 classification: Function, Seniority, and flags. Single source of truth."""
import re

SEP = "; "
def split(s):
    return [x for x in s.split(SEP) if x] if isinstance(s, str) else []

# ---------- FUNCTION ----------
TITLE_FIRST = [  # high-confidence title patterns; beat everything
    (r"deployment strategist|deployment lead", "Professional Services"),
    (r"forward[- ]deployed", "Engineering"),
    (r"\b(general manager|managing director|country (director|manager|lead|head)|regional head|unit head)\b"
     r"|^head of (latam|emea|apac|americas|europe|asia|anz|mena|dach|nordics)\b", "General Management"),
]
JT_OVERRIDE = {
    "Data Scientist": "Data & Analytics", "Data Analyst": "Data & Analytics", "Data Engineer": "Data & Analytics",
    "Research Scientist": "Engineering", "AI Engineer": "Engineering",
    "Product Manager": "Product", "Customer Success Manager": "Customer Success",
    "Solutions Architect": "Professional Services",
}
JF_MAP = {
    "Engineering": "Engineering", "Sales": "Sales", "Business Development": "Sales",
    "Marketing": "Marketing", "Public Relations": "Marketing", "Writing / Editing": "Marketing",
    "Design": "Design", "Art / Creative": "Design", "Product Management": "Product",
    "Customer Service": "Customer Success", "Accounting": "Finance", "Auditing": "Finance",
    "Finance": "Finance", "Human Resources": "People & Talent", "Legal": "Legal & Compliance",
    "Administrative": "Operations", "Supply Chain": "Operations", "Distribution": "Operations",
    "Operations": "Operations", "Project Management": "Operations", "Technician": "Operations",
    "Training": "Operations", "Consulting": "Professional Services",
    "Strategy / Planning": "Strategy & Business Development",
}  # Analyst, Science, Research, Other, Education, General Business → fall through
DEPT_EXTRA = {
    "DS": "Professional Services", "Safeguards (Trust & Safety)": "Legal & Compliance",
    "Public Policy": "Legal & Compliance", "Account Executive": "Sales",
    "Account Manager": "Sales", "Nursing": "Clinical",
}
TITLE_LAST = [  # checked after Consider + dept, before generic title inference
    (r"\b(nurse|nursing|rn|lpn|aprn|therapist|physician|clinician|caregiver|physiotherap\w*|pharmacist)\b"
     r"|1099 provider", "Clinical"),
    (r"supply chain|\behs\b|warehouse|material handler|logistics|shipyard|fleet|forklift", "Operations"),
    (r"\bdrafter\b|\bintegrator\b|\bcad\b|logiciel|d[ée]veloppeur|ing[ée]nieur", "Engineering"),
    (r"\bcomptes\b|\bventes\b", "Sales"),
    (r"instructor|learning (&|and) development|\bl&d\b", "People & Talent"),
]

def classify_function(title, dept, consider_func, consider_types, dept_fallback, title_fallback):
    """Returns (function, source). Fallbacks are the loader's normalize() and enhanced_infer_function()."""
    t = (title or "").lower()
    for pat, f in TITLE_FIRST:
        if re.search(pat, t): return f, "title"
    for jt in split(consider_types):
        if jt in JT_OVERRIDE: return JT_OVERRIDE[jt], "jobType"
    for jf in split(consider_func):
        if jf in JF_MAP: return JF_MAP[jf], "jobFunction"
    d = (dept or "").strip() if isinstance(dept, str) else ""
    if d:
        if d in DEPT_EXTRA: return DEPT_EXTRA[d], "dept"
        f = dept_fallback(d)
        if f and f != "Unknown": return f, "dept"
    for pat, f in TITLE_LAST:
        if re.search(pat, t): return f, "title"
    f = title_fallback(title or "")
    return (f, "title") if f and f != "Unknown" else ("Unknown", "none")

# ---------- SENIORITY ----------
SEN_MAP = {"Intern": "Intern", "Junior": "Junior", "Mid": "Mid", "Senior": "Senior", "Expert": "Expert",
           "Manager/Senior Manager": "Manager/Sr Manager", "Director/Senior Director": "Director/Sr Director",
           "VP/SVP": "VP", "C-Level / Executive": "C-Level"}
RANK = ["Intern", "Junior", "Mid", "Senior", "Expert", "Manager/Sr Manager",
        "Director/Sr Director", "VP", "C-Level"]
C_LEVEL = re.compile(r"\b(ceo|cto|cfo|coo|cpo|cro|cmo|ciso|cio)\b|(?<!vice )\bpresident\b|\bchief\b(?! of staff)")
VP = re.compile(r"\b(vp|svp|evp)\b|vice president")

def classify_seniority(title, consider_sen):
    t = (title or "").lower()
    if C_LEVEL.search(t): return "C-Level"
    if VP.search(t): return "VP"
    vals = [SEN_MAP[x] for x in split(consider_sen) if x in SEN_MAP]
    if vals: return max(vals, key=RANK.index)          # ranges like "Junior, Mid" → take highest
    if re.search(r"\bintern(ship)?\b", t): return "Intern"
    if "director" in t or t.startswith("head of"): return "Director/Sr Director"
    if re.search(r"\b(senior|sr\.?|staff|principal|lead)\b", t): return "Senior"
    if "manager" in t: return "Manager/Sr Manager"
    if re.search(r"\b(junior|jr\.?|associate|entry)\b", t): return "Junior"
    return "Unknown"

# ---------- FLAGS + JUNK ----------
JUNK = re.compile(r"general application|careers page|meet the firm|unsolicited|talent (pool|community)"
                  r"|future opportunit|don'?t see a role|sign up here|expression of interest", re.I)
def is_junk(title):
    return bool(JUNK.search(title or ""))

def flags(title, consider_types, seniority):
    t = (title or "").lower()
    types = split(consider_types)
    return {
        "Head of": t.startswith("head of") or " head of " in f" {t} ",
        "Manages People": "People Manager" in types or "Supervisor" in types
                          or seniority in ("Director/Sr Director", "VP", "C-Level"),
        "Forward Deployed": bool(re.search(r"forward[- ]deployed|deployment strategist|deployment lead", t)),
    }
