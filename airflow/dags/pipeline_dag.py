from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/Users/imac/Desktop/de project/job-market-pipeline")

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


def task_transform():
    mark_duplicates()
    raw_jobs = get_all_raw_jobs()
    success  = 0
    failed   = 0
    for raw_job in raw_jobs:
        try:
            cleaned    = clean_job(raw_job)
            cleaned_id = insert_cleaned_job(cleaned)
            text       = f"{raw_job.get('title','')} {raw_job.get('description','')}"
            skills     = extract_skills(text)
            if skills:
                insert_skills(cleaned_id, skills, raw_job["source"])
            success += 1
        except Exception as e:
            logger.error(f"Transform failed for job id={raw_job['id']}: {e}")
            failed += 1
    logger.info(f"Transform done — Success: {success} | Failed: {failed}")
    return success


# ============================================
# NEW — Google Sheets Export Task
# ============================================
def task_export_to_sheets():
    """Export all data to Google Sheets automatically"""
    from export_to_sheets import export_to_google_sheets, export_to_csv_backup

    # Upload to Google Sheets
    export_to_google_sheets()

    # Also save CSV as backup
    export_to_csv_backup()

    logger.info("✅ Data exported to Google Sheets + CSV backup")


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

    transform = PythonOperator(
        task_id           = "transform_jobs",
        python_callable   = task_transform,
        execution_timeout = timedelta(hours=1),
    )

    # NEW — Export to Google Sheets
    export_sheets = PythonOperator(
        task_id           = "export_to_google_sheets",
        python_callable   = task_export_to_sheets,
        execution_timeout = timedelta(minutes=15),
    )

    summary = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = task_summary,
    )

    # ============================================
    # Updated Pipeline Order
    # ============================================
    # OLD: setup_db >> scrape_naukri >> transform >> summary
    # NEW: setup_db >> scrape_naukri >> transform >> export_sheets >> summary

    setup_db >> scrape_naukri >> transform >> export_sheets >> summary