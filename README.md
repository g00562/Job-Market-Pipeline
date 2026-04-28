# Job Market Data Pipeline

An end-to-end Data Engineering pipeline that scrapes job listings from **Naukri**, **LinkedIn**, and **Indeed**, transforms and stores them in **PostgreSQL**, and exports insights to **Google Sheets**.

---

## Pipeline Flow

```
Scrape (Naukri + LinkedIn + Indeed)
        ↓
  PostgreSQL (jobs_raw)
        ↓
  Deduplicate → Clean → Extract Skills
        ↓
  PostgreSQL (jobs_cleaned + skills_extracted)
        ↓
  Google Sheets + CSV Export
```

Orchestrated by **Apache Airflow** on a daily schedule.

---

## Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Selenium, BeautifulSoup, Requests, RapidAPI |
| Storage | PostgreSQL |
| Transformation | Python, Pandas |
| Orchestration | Apache Airflow |
| Export | Google Sheets API, gspread |

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
│   ├── naukri_scraper.py      # Selenium
│   ├── linkedin_scraper.py    # Public API
│   └── indeed_scraper.py      # RapidAPI (JSearch)
│
├── transformers/
│   ├── cleaner.py             # Normalize titles, salary, location
│   ├── skill_extractor.py     # Tag 50+ DE skills
│   └── deduplicator.py
│
├── loaders/
│   └── postgres_loader.py     # DB operations + connection pooling
│
├── sql/
│   └── create_tables.sql      # Schema
│
└── airflow/
    └── dags/
        └── pipeline_dag.py    # DAG definition
```

---

## Setup

### 1. Prerequisites
- Python 3.10+
- PostgreSQL 13+
- Google Chrome (for Naukri scraper)

### 2. Install

```bash
git clone https://github.com/your-username/job-market-pipeline.git
cd job-market-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
# Fill in DB_PASSWORD (required), GOOGLE_SHEET_ID, RAPIDAPI_KEY
```

### 4. Database

```bash
createdb job_market_db
```

### 5. Run

```bash
python main.py
```

---

## Airflow (Optional)

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd):$PYTHONPATH

airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com

# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler
```

Open http://localhost:8080 → enable `job_market_pipeline` DAG.

---

## Google Sheets Export

Requires a Google Cloud service account with Sheets + Drive API enabled.

1. Download the JSON key → save to `credentials/google_sheets_creds.json`
2. Share your Google Sheet with the service account email
3. Set `GOOGLE_SHEET_ID` in `.env`

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
| `RAPIDAPI_KEY` | | For Indeed scraper |

---

## License

MIT
