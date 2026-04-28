import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from loaders.postgres_loader import insert_raw_job, job_exists
from loguru import logger
from dotenv import load_dotenv
import time
import random
from config import Config

# .env is already loaded by config, but we ensure it's loaded here too
load_dotenv()

SEARCH_KEYWORDS = [
    "data engineer",
    "etl developer",
    "data engineering",
]

LOCATIONS = [
    "Bangalore, India",
    "Hyderabad, India",
    "Pune, India",
    "Mumbai, India",
    "Chennai, India",
]

class IndeedScraper:
    def __init__(self):
        self.source  = "indeed"
        self.api_key = Config.RAPIDAPI_KEY
        
        if not self.api_key:
            logger.warning("⚠️ RAPIDAPI_KEY not configured - Indeed scraper will fail")
        
        self.headers = {
            "X-RapidAPI-Key":  self.api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }
        self.base_url = "https://jsearch.p.rapidapi.com/search"

    def get_jobs(self, keyword: str, location: str, page: int = 1) -> list:
        params = {
            "query":        f"{keyword} in {location}",
            "page":         str(page),
            "num_pages":    "1",
            "date_posted":  "week",
        }
        try:
            time.sleep(random.uniform(Config.SCRAPER_DELAY_MIN, Config.SCRAPER_DELAY_MAX))
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=Config.SCRAPER_TIMEOUT
            )
            if response.status_code != 200:
                logger.warning(f"⚠️ Indeed API returned {response.status_code}")
                return []

            data = response.json()
            return self.parse_jobs(data.get("data", []))

        except Exception as e:
            logger.error(f"❌ Indeed API error: {e}")
            return []

    def parse_jobs(self, job_list: list) -> list:
        jobs = []
        logger.info(f"Found {len(job_list)} Indeed jobs from API")

        for item in job_list:
            try:
                title   = item.get("job_title", "")
                url     = item.get("job_apply_link", "") or item.get("job_url", "")
                company = item.get("employer_name", "")
                city    = item.get("job_city", "") or item.get("job_state", "")
                salary  = item.get("job_salary", "Not disclosed") or "Not disclosed"
                desc    = item.get("job_description", "")[:500]  # Limit length

                if not title or not url:
                    continue

                if job_exists(url):
                    logger.info(f"Skipping duplicate: {title}")
                    continue

                jobs.append({
                    "source":      "indeed",
                    "title":       title,
                    "company":     company,
                    "location":    city,
                    "salary_raw":  str(salary),
                    "experience":  None,
                    "description": desc,
                    "url":         url,
                })

            except Exception as e:
                logger.warning(f"Error parsing Indeed job: {e}")
                continue

        return jobs

    def scrape(self):
        if not self.api_key:
            logger.error("❌ RAPIDAPI_KEY not configured - cannot scrape Indeed")
            return 0
        
        total_inserted = 0

        for keyword in SEARCH_KEYWORDS:
            for location in LOCATIONS:
                for page in range(1, Config.INDEED_PAGES + 1):
                    logger.info(f"🔍 Scraping Indeed API: {keyword} | {location} | page {page}")
                    try:
                        jobs = self.get_jobs(keyword, location, page)

                        for job in jobs:
                            try:
                                insert_raw_job(job)
                                total_inserted += 1
                            except Exception as e:
                                logger.warning(f"⚠️ Insert failed: {e}")

                        time.sleep(random.uniform(Config.SCRAPER_DELAY_MIN, Config.SCRAPER_DELAY_MAX))
                    except Exception as e:
                        logger.error(f"❌ Error scraping page: {e}")
                        continue

        logger.info(f"✅ Indeed API scraping done. Total inserted: {total_inserted}")
        return total_inserted


if __name__ == "__main__":
    scraper = IndeedScraper()
    scraper.scrape()