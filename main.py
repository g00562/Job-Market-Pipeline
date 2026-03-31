from scrapers.naukri_scraper import NaukriScraper
from transformers.cleaner import clean_job
from transformers.skill_extractor import extract_skills
from transformers.deduplicator import get_all_raw_jobs, mark_duplicates
from loaders.postgres_loader import (
    create_tables, insert_cleaned_job, insert_skills
)
from loguru import logger

def run_scraper():
    logger.info("Starting Naukri scraper...")
    scraper = NaukriScraper()
    total   = scraper.scrape()
    logger.info(f"Naukri scraping done — {total} jobs inserted")
    return total

def run_transformation():
    mark_duplicates()
    raw_jobs = get_all_raw_jobs()
    logger.info(f"Transforming {len(raw_jobs)} jobs...")
    success = 0
    failed  = 0
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

if __name__ == "__main__":
    create_tables()
    run_scraper()
    run_transformation()
    logger.info("Pipeline complete!")