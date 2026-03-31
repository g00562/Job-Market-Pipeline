# 🔍 Naukri Job Market Intelligence Pipeline

An automated end-to-end Data Engineering pipeline that scrapes,
transforms, and visualizes Data Engineer job market data from Naukri.com.

---

## 📊 Live Dashboard
[View Live Dashboard](https://lookerstudio.google.com/reporting/87d61d00-e46f-426a-be14-fae8d830b5dd)

---

## 🏗️ Architecture
```
Naukri.com
    ↓
Selenium Scraper (Python)
    ↓
PostgreSQL — jobs_raw
    ↓
Transformation Layer (Pandas)
├── Data Cleaner
├── Skill Extractor
└── Deduplicator
    ↓
PostgreSQL — jobs_cleaned / skills / companies
    ↓
Apache Airflow (Daily at 9 AM)
    ↓
Looker Studio Dashboard
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Scraping | Python, Selenium, BeautifulSoup |
| Transformation | Python, Pandas |
| Database | PostgreSQL |
| Orchestration | Apache Airflow |
| Visualization | Looker Studio |
| Environment | Python venv, dotenv |

---

## ✨ Features

- Scrapes 600+ Data Engineer job postings daily from Naukri.com
- Selenium-based scraper handles JavaScript-rendered pages
- Automatic duplicate detection using URL-based checks
- Data cleaning — normalizes job titles, cities, salary ranges
- Skill extraction — auto-tags 50+ DE skills (Python, Spark, Airflow, dbt, etc.)
- Salary normalization — converts raw salary strings to min/max LPA values
- 4-table PostgreSQL data warehouse (raw, cleaned, skills, companies)
- Apache Airflow DAG with 4 tasks — runs automatically every day at 9 AM
- Email alerting on pipeline failure with auto-retry logic
- Looker Studio dashboard with 6 interactive charts
- CSV export for external analysis

---

## 📁 Project Structure
```
job-market-pipeline/
│
├── main.py                    ← Pipeline entry point
├── export_to_csv.py           ← Export data to CSV for dashboard
├── requirements.txt           ← Python dependencies
│
├── scrapers/
│   ├── base_scraper.py        ← Base scraper class
│   └── naukri_scraper.py      ← Naukri.com Selenium scraper
│
├── transformers/
│   ├── cleaner.py             ← Data cleaning & normalization
│   ├── skill_extractor.py     ← DE skill extraction from JDs
│   └── deduplicator.py        ← Cross-run deduplication
│
├── loaders/
│   └── postgres_loader.py     ← PostgreSQL CRUD operations
│
├── sql/
│   └── create_tables.sql      ← Database schema
│
├── logs/                      ← Pipeline logs
│
└── airflow/
    └── dags/
        └── pipeline_dag.py    ← Airflow DAG definition
```

---

## 🗄️ Database Schema
```sql
jobs_raw          -- Raw scraped data from Naukri
jobs_cleaned      -- Transformed and normalized jobs
skills_extracted  -- Skills tagged from job descriptions
companies         -- Company metadata
```

---

## 📈 Dashboard Insights

- Top 20 most in-demand skills for Data Engineers
- Jobs by city (Bangalore, Hyderabad, Pune, Mumbai, Chennai)
- Average salary range by city (in LPA)
- Top hiring companies
- Experience level distribution (Fresher → Expert)
- Daily job posting trends

---

## 🚀 How to Run

### Prerequisites
- Python 3.10+
- PostgreSQL 15
- Google Chrome (for Selenium)
- Apache Airflow 3.x

### Setup
```bash
# 1. Clone the repository
git clone https://github.com/g00562/Job-Market-Pipeline.git
cd Job-Market-Pipeline

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 5. Create database
psql postgres -c "CREATE DATABASE job_market_db;"

# 6. Run the pipeline
python main.py
```

### Run with Airflow
```bash
# Start Airflow
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone

# Open browser: http://localhost:8080
# Trigger: job_market_pipeline DAG
```

---

## ⚙️ Configuration

Create a `.env` file in the root directory:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=job_market_db
DB_USER=your_username
DB_PASSWORD=your_password
```

---

## 📦 Requirements
```
selenium
webdriver-manager
beautifulsoup4
requests
pandas
numpy
psycopg2-binary
sqlalchemy
apache-airflow
apache-airflow-providers-standard
python-dotenv
loguru
lxml
```

---

## 🔍 Sample Data
```
Title             : Senior Data Engineer
Company           : Infosys
City              : Bangalore
Experience        : 3-6 years
Salary            : 12-18 LPA
Skills            : Python, Spark, Airflow, SQL, AWS
```

---

## 📊 Pipeline Stats

- Jobs scraped: 900+
- Skills tracked: 50+
- Cities covered: 5 (Bangalore, Hyderabad, Pune, Mumbai, Chennai)
- Pipeline runtime: ~8 minutes
- Schedule: Daily at 9:00 AM IST

---

## 🙋 Author

**Ishan**
- LinkedIn: [Ishan](https://linkedin.com/in/ishan-hirani)
- GitHub: [Ishan](https://github.com/Ishanhirani11)
- Email: patelishan@gmail.com

---

## 📄 License

MIT License — feel free to use this project for learning and portfolio purposes.
