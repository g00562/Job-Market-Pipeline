import re
from loguru import logger

# ── Relevant DE job title keywords ──
RELEVANT_TITLES = [
    "data engineer",
    "data engineering",
    "etl developer",
    "etl engineer",
    "data pipeline",
    "big data engineer",
    "data platform engineer",
    "cloud data engineer",
    "analytics engineer",
    "data warehouse engineer",
    "dwh engineer",
    "azure data engineer",
    "aws data engineer",
    "gcp data engineer",
    "databricks engineer",
    "data integration engineer",
]

# ── Title standardization map ──
TITLE_STANDARDS = {
    "sr.":    "senior",
    "sr ":    "senior ",
    "jr.":    "junior",
    "jr ":    "junior ",
    "lead de": "lead data engineer",
}

def is_relevant_job(title: str) -> bool:
    """Returns True only if job title is relevant to Data Engineering."""
    if not title:
        return False
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in RELEVANT_TITLES)

def clean_title(title: str) -> str:
    if not title:
        return None
    title = re.sub(r"[^\w\s\-\/]", "", title)
    title = re.sub(r"\s+", " ", title).strip().lower()
    for old, new in TITLE_STANDARDS.items():
        title = title.replace(old, new)
    return title.title()

def clean_company(company: str) -> str:
    if not company:
        return None
    company = re.sub(r"\s+", " ", company).strip()
    for word in ["Hiring", "Urgent", "Walk-in", "Opening"]:
        company = company.replace(word, "").strip()
    return company

def clean_location(location: str):
    if not location:
        return None, False
    location  = location.lower().strip()
    is_remote = any(w in location for w in ["remote", "work from home", "wfh"])
    city_map  = {
        "bangalore": "Bangalore", "bengaluru": "Bangalore",
        "hyderabad": "Hyderabad", "pune":      "Pune",
        "mumbai":    "Mumbai",    "chennai":   "Chennai",
        "delhi":     "Delhi",     "noida":     "Noida",
        "gurgaon":   "Gurgaon",  "gurugram":  "Gurgaon",
        "kolkata":   "Kolkata",  "ahmedabad": "Ahmedabad",
        "surat":     "Surat",    "remote":    "Remote",
    }
    for key, value in city_map.items():
        if key in location:
            return value, is_remote
    return location.title(), is_remote

def parse_salary(salary_raw: str) -> tuple:
    if not salary_raw or salary_raw.lower() in ["not disclosed", "not mentioned", ""]:
        return None, None
    salary_raw = salary_raw.lower().replace(",", "").strip()
    lpa_match  = re.findall(r"(\d+(?:\.\d+)?)", salary_raw)
    if not lpa_match:
        return None, None
    values     = [float(x) for x in lpa_match]
    multiplier = 100000
    if len(values) >= 2:
        return int(values[0] * multiplier), int(values[1] * multiplier)
    elif len(values) == 1:
        return int(values[0] * multiplier), int(values[0] * multiplier)
    return None, None

def parse_experience(experience: str) -> tuple:
    if not experience:
        return None, None
    numbers = re.findall(r"(\d+)", experience)
    if len(numbers) >= 2:
        return int(numbers[0]), int(numbers[1])
    elif len(numbers) == 1:
        return int(numbers[0]), int(numbers[0])
    return None, None

def clean_job(raw_job: dict) -> dict | None:
    # Filter irrelevant jobs first
    if not is_relevant_job(raw_job.get("title")):
        logger.debug(f"Skipping irrelevant: {raw_job.get('title')}")
        return None

    city, is_remote        = clean_location(raw_job.get("location"))
    salary_min, salary_max = parse_salary(raw_job.get("salary_raw"))
    exp_min, exp_max       = parse_experience(raw_job.get("experience"))

    return {
        "raw_id":         raw_job["id"],
        "title_std":      clean_title(raw_job.get("title")),
        "company":        clean_company(raw_job.get("company")),
        "city":           city,
        "is_remote":      is_remote,
        "salary_min":     salary_min,
        "salary_max":     salary_max,
        "experience_min": exp_min,
        "experience_max": exp_max,
        "source":         raw_job.get("source"),
    }