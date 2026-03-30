from loaders.postgres_loader import (
    create_tables, insert_cleaned_job, insert_skills
)
from transformers.cleaner import clean_job
from transformers.skill_extractor import extract_skills
from transformers.deduplicator import get_all_raw_jobs, mark_duplicates
from loguru import logger

def run_transformation():
    logger.info("Starting transformation pipeline...")

    # Step 1 — Check duplicates across platforms
    mark_duplicates()

    # Step 2 — Fetch all untransformed raw jobs
    raw_jobs = get_all_raw_jobs()
    logger.info(f"Transforming {len(raw_jobs)} jobs...")

    success = 0
    failed  = 0

    for raw_job in raw_jobs:
        try:
            # Clean the job
            cleaned = clean_job(raw_job)

            # Insert into jobs_cleaned
            cleaned_id = insert_cleaned_job(cleaned)

            # Extract and insert skills from title + description
            text   = f"{raw_job.get('title','')} {raw_job.get('description','')}"
            skills = extract_skills(text)
            if skills:
                insert_skills(cleaned_id, skills, raw_job["source"])

            success += 1

        except Exception as e:
            logger.error(f"Transform failed for job id={raw_job['id']}: {e}")
            failed += 1

    logger.info(f"Transformation complete — Success: {success} | Failed: {failed}")

if __name__ == "__main__":
    create_tables()
    run_transformation()