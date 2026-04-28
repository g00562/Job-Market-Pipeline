from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Get project root dynamically (2 levels up from dags directory)
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.naukri_scraper import NaukriScraper
from transformers.cleaner import clean_job
from transformers.skill_extractor import extract_skills
from transformers.deduplicator import get_all_raw_jobs, mark_duplicates
from loaders.postgres_loader import (
    create_tables, insert_cleaned_job, insert_skills
)
from loguru import logger

default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


def task_setup_db():
    create_tables()
    logger.info("Database ready")


def task_scrape_naukri():
    scraper = NaukriScraper()
    total   = scraper.scrape()
    logger.info(f"Naukri scrape done — {total} jobs inserted")
    return total


def task_scrape_linkedin():
    from scrapers.linkedin_scraper import LinkedInScraper
    scraper = LinkedInScraper()
    total   = scraper.scrape()
    logger.info(f"LinkedIn scrape done — {total} jobs inserted")
    return total


def task_scrape_indeed():
    from scrapers.indeed_scraper import IndeedScraper
    scraper = IndeedScraper()
    total   = scraper.scrape()
    logger.info(f"Indeed scrape done — {total} jobs inserted")
    return total


def task_transform():
    mark_duplicates()
    raw_jobs = get_all_raw_jobs()
    success  = 0
    failed   = 0
    skipped  = 0
    for raw_job in raw_jobs:
        try:
            cleaned = clean_job(raw_job)
            if cleaned is None:
                skipped += 1
                continue
            cleaned_id = insert_cleaned_job(cleaned)
            text       = f"{raw_job.get('title','')} {raw_job.get('description','')}"
            skills     = extract_skills(text)
            if skills:
                insert_skills(cleaned_id, skills, raw_job["source"])
            success += 1
        except Exception as e:
            logger.error(f"Transform failed for job id={raw_job['id']}: {e}")
            failed += 1
    logger.info(f"Transform done — Success: {success} | Skipped: {skipped} | Failed: {failed}")
    return success


def task_export_to_sheets():
    """Export all data to Google Sheets automatically"""
    try:
        from export_to_sheets import export_to_google_sheets, export_to_csv_backup
        export_to_google_sheets()
        export_to_csv_backup()
        logger.info("✅ Data exported to Google Sheets + CSV backup")
    except Exception as e:
        logger.error(f"❌ Export to sheets failed: {e}")
        raise


def task_summary():
    from loaders.postgres_loader import get_connection
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM jobs_raw")
        total_raw = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM jobs_cleaned")
        total_cleaned = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM skills_extracted")
        total_skills = cur.fetchone()[0]

        cur.execute("""
            SELECT source, COUNT(*) as cnt
            FROM jobs_raw
            GROUP BY source
            ORDER BY cnt DESC
        """)
        by_source = cur.fetchall()

        cur.execute("""
            SELECT skill, COUNT(*) as cnt
            FROM skills_extracted
            GROUP BY skill
            ORDER BY cnt DESC
            LIMIT 5
        """)
        top_skills = cur.fetchall()

        logger.info("=" * 50)
        logger.info(f"PIPELINE SUMMARY — {datetime.now().date()}")
        logger.info(f"Total raw jobs     : {total_raw}")
        logger.info(f"Total cleaned jobs : {total_cleaned}")
        logger.info(f"Total skills tagged: {total_skills}")
        logger.info("Jobs by source:")
        for source, count in by_source:
            logger.info(f"  {source}: {count}")
        logger.info("Top 5 skills:")
        for skill, count in top_skills:
            logger.info(f"  {skill}: {count}")
        logger.info("=" * 50)
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id           = "job_market_pipeline",
    default_args     = default_args,
    description      = "Daily job market scraping, transformation, and export pipeline",
    schedule         = "0 9 * * *",
    start_date       = datetime(2026, 3, 27),
    catchup          = False,
    tags             = ["job-market", "scraping", "etl"],
) as dag:

    setup_db = PythonOperator(
        task_id         = "setup_database",
        python_callable = task_setup_db,
    )

    scrape_naukri = PythonOperator(
        task_id           = "scrape_naukri",
        python_callable   = task_scrape_naukri,
        execution_timeout = timedelta(hours=2),
    )

    scrape_linkedin = PythonOperator(
        task_id           = "scrape_linkedin",
        python_callable   = task_scrape_linkedin,
        execution_timeout = timedelta(hours=1),
    )

    scrape_indeed = PythonOperator(
        task_id           = "scrape_indeed",
        python_callable   = task_scrape_indeed,
        execution_timeout = timedelta(hours=1),
    )

    transform = PythonOperator(
        task_id           = "transform_jobs",
        python_callable   = task_transform,
        execution_timeout = timedelta(hours=1),
    )

    export_sheets = PythonOperator(
        task_id           = "export_to_google_sheets",
        python_callable   = task_export_to_sheets,
        execution_timeout = timedelta(minutes=15),
    )

    summary = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = task_summary,
    )

    # Pipeline order:
    # setup_db → [scrape_naukri, scrape_linkedin, scrape_indeed] (parallel)
    #          → transform → export_sheets → summary

    setup_db >> [scrape_naukri, scrape_linkedin, scrape_indeed] >> transform >> export_sheets >> summary