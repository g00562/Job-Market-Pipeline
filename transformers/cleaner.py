import re
from loguru import logger

def clean_title(title: str) -> str:
    if not title:
        return None
    # Remove special characters, extra spaces
    title = re.sub(r"[^\w\s\-\/]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    # Standardize common variations
    title = title.replace("Sr.", "Senior").replace("Jr.", "Junior")
    return title.title()

def clean_company(company: str) -> str:
    if not company:
        return None
    company = re.sub(r"\s+", " ", company).strip()
    # Remove common suffixes noise
    noise = ["Hiring", "Urgent", "Walk-in", "Opening"]
    for word in noise:
        company = company.replace(word, "").strip()
    return company

def clean_location(location: str) -> str | None:
    if not location:
        return None, False

    location = location.lower().strip()
    is_remote = any(word in location for word in ["remote", "work from home", "wfh"])

    # Extract city name
    city_map = {
        "bangalore": "Bangalore", "bengaluru": "Bangalore",
        "hyderabad": "Hyderabad", "pune": "Pune",
        "mumbai": "Mumbai",       "chennai": "Chennai",
        "delhi": "Delhi",         "noida": "Noida",
        "gurgaon": "Gurgaon",    "gurugram": "Gurgaon",
        "kolkata": "Kolkata",     "ahmedabad": "Ahmedabad",
        "surat": "Surat",         "remote": "Remote",
    }

    for key, value in city_map.items():
        if key in location:
            return value, is_remote

    # Return cleaned raw if no match
    return location.title(), is_remote

def parse_salary(salary_raw: str) -> tuple:
    if not salary_raw or salary_raw.lower() in ["not disclosed", "not mentioned", ""]:
        return None, None

    salary_raw = salary_raw.lower().replace(",", "").strip()

    # Match patterns like "8-12 lpa", "10 lpa", "8 to 12 lakhs"
    lpa_match = re.findall(r"(\d+(?:\.\d+)?)", salary_raw)

    if not lpa_match:
        return None, None

    values = [float(x) for x in lpa_match]

    # Convert LPA to annual rupees
    multiplier = 100000  # 1 LPA = 1,00,000 INR

    if len(values) >= 2:
        return int(values[0] * multiplier), int(values[1] * multiplier)
    elif len(values) == 1:
        return int(values[0] * multiplier), int(values[0] * multiplier)

    return None, None

def parse_experience(experience: str) -> tuple:
    if not experience:
        return None, None

    # Match patterns like "2-5 Yrs", "0-1 Year", "3 Years"
    numbers = re.findall(r"(\d+)", experience)

    if len(numbers) >= 2:
        return int(numbers[0]), int(numbers[1])
    elif len(numbers) == 1:
        return int(numbers[0]), int(numbers[0])

    return None, None

def clean_job(raw_job: dict) -> dict:
    city, is_remote = clean_location(raw_job.get("location"))
    salary_min, salary_max = parse_salary(raw_job.get("salary_raw"))
    exp_min, exp_max = parse_experience(raw_job.get("experience"))

    cleaned = {
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

    logger.debug(f"Cleaned job: {cleaned['title_std']} @ {cleaned['company']}")
    return cleaned