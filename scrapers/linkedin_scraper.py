import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup
from loaders.postgres_loader import insert_raw_job, job_exists
from loguru import logger
import time
import random

SEARCH_KEYWORDS = [
    "data engineer",
    "data engineering",
    "etl developer",
]

LOCATIONS = [
    "Bangalore India",
    "Hyderabad India",
    "Pune India",
    "Mumbai India",
    "Chennai India",
]

class LinkedInScraper:
    def __init__(self):
        self.source  = "linkedin"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        # LinkedIn public guest API — no login needed
        self.base_url = (
            "https://www.linkedin.com/jobs-guest/jobs/api"
            "/seeMoreJobPostings/search"
        )

    def get_jobs(self, keyword: str, location: str, start: int = 0) -> list:
        params = {
            "keywords": keyword,
            "location": location,
            "start":    start,
            "f_TPR":    "r604800",  # Posted in last 7 days
        }
        try:
            time.sleep(random.uniform(2, 4))
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=15
            )
            if response.status_code != 200:
                logger.warning(f"LinkedIn returned {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, "lxml")
            return self.parse_jobs(soup)

        except Exception as e:
            logger.error(f"LinkedIn fetch error: {e}")
            return []

    def parse_jobs(self, soup) -> list:
        jobs  = []
        cards = soup.find_all("div", class_="base-card")
        logger.info(f"Found {len(cards)} LinkedIn job cards")

        for card in cards:
            try:
                # Title
                title_tag = card.find("h3", class_="base-search-card__title")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)

                # URL
                link_tag = card.find("a", class_="base-card__full-link")
                url      = link_tag.get("href", "").split("?")[0] if link_tag else ""

                if url and job_exists(url):
                    logger.info(f"Skipping duplicate: {title}")
                    continue

                # Company
                company_tag = card.find("h4", class_="base-search-card__subtitle")
                company     = company_tag.get_text(strip=True) if company_tag else None

                # Location
                loc_tag  = card.find("span", class_="job-search-card__location")
                location = loc_tag.get_text(strip=True) if loc_tag else None

                if not title:
                    continue

                jobs.append({
                    "source":      "linkedin",
                    "title":       title,
                    "company":     company,
                    "location":    location,
                    "salary_raw":  "Not disclosed",
                    "experience":  None,
                    "description": "",
                    "url":         url,
                })

            except Exception as e:
                logger.warning(f"Error parsing LinkedIn card: {e}")
                continue

        return jobs

    def scrape(self):
        total_inserted = 0

        for keyword in SEARCH_KEYWORDS:
            for location in LOCATIONS:
                for page in range(0, 3):
                    start = page * 25
                    logger.info(f"Scraping LinkedIn: {keyword} | {location} | page {page+1}")
                    jobs  = self.get_jobs(keyword, location, start)

                    for job in jobs:
                        try:
                            insert_raw_job(job)
                            total_inserted += 1
                        except Exception as e:
                            logger.error(f"Insert failed: {e}")

                    time.sleep(random.uniform(3, 6))

        logger.info(f"LinkedIn scraping done. Total inserted: {total_inserted}")
        return total_inserted


if __name__ == "__main__":
    scraper = LinkedInScraper()
    scraper.scrape()
