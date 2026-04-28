import sys
from pathlib import Path
from loguru import logger

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from scrapers.naukri_scraper import NaukriScraper
from scrapers.linkedin_scraper import LinkedInScraper
from scrapers.indeed_scraper import IndeedScraper
from transformers.cleaner import clean_job
from transformers.skill_extractor import extract_skills
from transformers.deduplicator import get_all_raw_jobs, mark_duplicates
from loaders.postgres_loader import (
    create_tables, insert_cleaned_job, insert_skills, close_connection_pool
)
from config import Config

# Configure logging
logger.add(
    Config.LOGS_DIR / "pipeline_{time}.log",
    level=Config.LOG_LEVEL,
    retention="30 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
)

def run_scrapers():
    """Run all scrapers with error handling"""
    results = {"naukri": 0, "linkedin": 0, "indeed": 0}
    
    try:
        # Naukri
        logger.info("🔍 Starting Naukri scraper...")
        try:
            naukri = NaukriScraper()
            results["naukri"] = naukri.scrape()
            logger.info(f"✅ Naukri done — {results['naukri']} jobs")
        except Exception as e:
            logger.error(f"❌ Naukri scraper failed: {e}")
            # Don't stop pipeline on scraper failure

        # LinkedIn
        logger.info("🔍 Starting LinkedIn scraper...")
        try:
            linkedin = LinkedInScraper()
            results["linkedin"] = linkedin.scrape()
            logger.info(f"✅ LinkedIn done — {results['linkedin']} jobs")
        except Exception as e:
            logger.error(f"❌ LinkedIn scraper failed: {e}")

        # Indeed
        logger.info("🔍 Starting Indeed scraper...")
        try:
            indeed = IndeedScraper()
            results["indeed"] = indeed.scrape()
            logger.info(f"✅ Indeed done — {results['indeed']} jobs")
        except Exception as e:
            logger.error(f"❌ Indeed scraper failed: {e}")

        total = sum(results.values())
        logger.info(f"📊 Total jobs scraped: {total}")
        return results["naukri"], results["linkedin"], results["indeed"]
        
    except Exception as e:
        logger.error(f"❌ Scraping pipeline failed: {e}")
        raise

def run_transformation():
    """Transform and clean jobs with comprehensive error handling"""
    try:
        logger.info("🔄 Starting transformation pipeline...")
        
        # Mark duplicates
        duplicate_count = mark_duplicates()
        
        # Fetch jobs to transform
        raw_jobs = get_all_raw_jobs(exclude_duplicates=True)
        logger.info(f"📋 Transforming {len(raw_jobs)} relevant jobs...")
        
        success = 0
        failed = 0
        skipped = 0

        for i, raw_job in enumerate(raw_jobs, 1):
            try:
                # Clean job
                cleaned = clean_job(raw_job)
                if cleaned is None:
                    skipped += 1
                    logger.debug(f"⏭️ Skipped irrelevant job: {raw_job.get('title', 'N/A')[:40]}")
                    continue
                
                # Insert cleaned job
                cleaned_id = insert_cleaned_job(cleaned)
                
                # Extract and insert skills
                text = f"{raw_job.get('title', '')} {raw_job.get('description', '')}"
                skills = extract_skills(text)
                if skills:
                    insert_skills(cleaned_id, skills, raw_job["source"])
                
                success += 1
                if i % 100 == 0:
                    logger.info(f"📈 Progress: {i}/{len(raw_jobs)} jobs processed")
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ Transform failed for job id={raw_job.get('id', 'N/A')}: {e}")
                continue

        logger.info("=" * 60)
        logger.info("🏁 TRANSFORMATION COMPLETE")
        logger.info(f"  ✅ Success:  {success}")
        logger.info(f"  ⏭️ Skipped:  {skipped}")
        logger.info(f"  ❌ Failed:   {failed}")
        logger.info(f"  🔗 Duplicates marked: {duplicate_count}")
        logger.info("=" * 60)
        
        return success, skipped, failed
        
    except Exception as e:
        logger.error(f"❌ Transformation pipeline failed: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("🚀 JOB MARKET PIPELINE STARTED")
        logger.info("=" * 60)
        
        # Setup database
        logger.info("📦 Setting up database...")
        create_tables()
        
        # Run scrapers
        n, l, i = run_scrapers()
        
        # Run transformation
        success, skipped, failed = run_transformation()
        
        # Final summary
        logger.info("=" * 60)
        logger.info("✅ PIPELINE EXECUTION COMPLETE")
        logger.info(f"  Naukri:    {n:5d} jobs")
        logger.info(f"  LinkedIn:  {l:5d} jobs")
        logger.info(f"  Indeed:    {i:5d} jobs")
        logger.info(f"  Total:     {n+l+i:5d} jobs scraped")
        logger.info(f"  Transformed: Success={success} | Skipped={skipped} | Failed={failed}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.critical(f"❌ PIPELINE FAILED: {e}")
        exit(1)
    finally:
        # Cleanup resources
        try:
            close_connection_pool()
        except:
            pass
        logger.info("✅ Pipeline cleanup complete")