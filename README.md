# 🔍 Job Market Intelligence Pipeline

An end-to-end Data Engineering pipeline that scrapes Data Engineering job listings from **Naukri** and **LinkedIn**, transforms and stores them in **PostgreSQL**, and visualizes insights on a live dashboard.

📊 **[Live Dashboard →](https://datastudio.google.com/reporting/d50a8ab8-3a76-4231-9b4e-8f4eecba8298)**

---

## Pipeline Architecture

```
Scrape (Naukri + LinkedIn)
        ↓
  PostgreSQL (jobs_raw)
        ↓
  Deduplicate → Clean → Extract Skills
        ↓
  PostgreSQL (jobs_cleaned + skills_extracted)
        ↓
  Google Sheets → Looker Studio Dashboard
```

Orchestrated by **Apache Airflow** on a daily schedule.

---

## Dashboard

The live Looker Studio dashboard has 3 pages:

| Page | Content |
|---|---|
| Overview | Total jobs, top cities, jobs by source |
| Skills & Experience | In-demand skills, experience levels, remote vs on-site |
| Top Companies | Top hiring companies with job counts |

---

## Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Selenium, BeautifulSoup, Requests, RapidAPI |
| Storage | PostgreSQL |
| Transformation | Python, Pandas |
| Orchestration | Apache Airflow |
| Export | Google Sheets API, gspread |
| Visualization | Looker Studio |

---

## Project Structure

```
job-market-pipeline/
├── main.py                    # Run pipeline directly
├── config.py                  # Config & env validation
├── export_to_sheets.py        # Export to Google Sheets + CSV
├── requirements.txt
│
├── scrapers/
│   ├── naukri_scraper.py      # Selenium-based scraper
│   ├── linkedin_scraper.py    # Public API scraper
│   └── indeed_scraper.py      # RapidAPI (JSearch) with retry logic
│
├── transformers/
│   ├── cleaner.py             # Normalize titles, salary, location, remote detection
│   ├── skill_extractor.py     # Tags 50+ DE skills from job descriptions
│   └── deduplicator.py        # Cross-source duplicate detection
│
├── loaders/
│   └── postgres_loader.py     # DB operations + connection pooling
│
├── sql/
│   └── create_tables.sql      # Schema definition
│
└── airflow/
    └── dags/
        └── pipeline_dag.py    # Airflow DAG definition
```

---

## Setup

### Prerequisites
- Python 3.10+
- PostgreSQL 13+
- Google Chrome (for Naukri scraper)

### Install

```bash
git clone https://github.com/g00562/Job-Market-Pipeline.git
cd job-market-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Configure

```bash
cp .env.example .env
# Fill in DB_PASSWORD, GOOGLE_SHEET_ID, RAPIDAPI_KEY
```

### Database

```bash
createdb job_market_db
psql -U postgres -d job_market_db -f sql/create_tables.sql
```

### Run

```bash
python main.py
```

---

## Airflow (Optional)

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd):$PYTHONPATH
airflow db migrate
airflow standalone
```

Open http://localhost:8080 → enable `job_market_pipeline` DAG.

---

## Google Sheets Export

1. Create a Google Cloud service account with Sheets + Drive API enabled
2. Download JSON key → save to `credentials/google_sheets_creds.json`
3. Share your Google Sheet with the service account email
4. Set `GOOGLE_SHEET_ID` in `.env`

```bash
python export_to_sheets.py
```

Exports 6 tabs: `jobs_cleaned`, `top_skills`, `jobs_by_city`, `salary_by_city`, `top_companies`, `experience_distribution`.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DB_PASSWORD` | ✅ | PostgreSQL password |
| `DB_HOST` | | Default: `localhost` |
| `DB_NAME` | | Default: `job_market_db` |
| `GOOGLE_SHEET_ID` | | For Sheets export |
| `GOOGLE_SHEETS_CREDS` | | Path to service account JSON |
| `RAPIDAPI_KEY` | | For Indeed scraper (RapidAPI JSearch) |
| `INDEED_PAGES_PER_LOCATION` | | Set to `0` to disable Indeed |

---

## License

MIT
