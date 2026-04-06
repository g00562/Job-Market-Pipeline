from scrapers.naukri_scraper import NaukriScraper
from scrapers.linkedin_scraper import LinkedInScraper
from scrapers.indeed_scraper import IndeedScraper
from transformers.cleaner import clean_job
from transformers.skill_extractor import extract_skills
from transformers.deduplicator import get_all_raw_jobs, mark_duplicates
from loaders.postgres_loader import (
    create_tables, insert_cleaned_job, insert_skills
)
from loguru import logger

def run_scrapers():
    # Naukri
    logger.info("Starting Naukri scraper...")
    naukri = NaukriScraper()
    n_total = naukri.scrape()
    logger.info(f"Naukri done — {n_total} jobs")

    # LinkedIn
    logger.info("Starting LinkedIn scraper...")
    linkedin = LinkedInScraper()
    l_total  = linkedin.scrape()
    logger.info(f"LinkedIn done — {l_total} jobs")

    # Indeed
    logger.info("Starting Indeed scraper...")
    indeed  = IndeedScraper()
    i_total = indeed.scrape()
    logger.info(f"Indeed done — {i_total} jobs")

    return n_total, l_total, i_total

def run_transformation():
    mark_duplicates()
    raw_jobs = get_all_raw_jobs()
    logger.info(f"Transforming {len(raw_jobs)} relevant jobs...")
    success = 0
    failed  = 0
    skipped = 0

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

if __name__ == "__main__":
    create_tables()
    n, l, i = run_scrapers()
    run_transformation()
    logger.info(f"Pipeline complete! Naukri: {n} | LinkedIn: {l} | Indeed: {i}")