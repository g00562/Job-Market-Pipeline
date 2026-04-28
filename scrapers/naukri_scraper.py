import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from loaders.postgres_loader import insert_raw_job, job_exists
from loguru import logger
import time
import random
from config import Config

SEARCH_KEYWORDS = [
    "data-engineer",
    "data-engineering",
    "etl-developer",
]

LOCATIONS = [
    "bangalore",
    "hyderabad",
    "pune",
    "mumbai",
    "chennai",
]

class NaukriScraper:
    def __init__(self):
        self.source = "naukri"
        self.driver = self._init_driver()
        self.pages_per_location = Config.NAUKRI_PAGES

    def _init_driver(self):
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            logger.info("✅ Chrome driver initialized")
            return driver
        except Exception as e:
            logger.error(f"❌ Failed to initialize Chrome driver: {e}")
            raise

    def get_page_source(self, url: str) -> BeautifulSoup | None:
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, Config.SCRAPER_TIMEOUT).until(
                EC.presence_of_element_located((By.CLASS_NAME, "srp-jobtuple-wrapper"))
            )
            time.sleep(random.uniform(Config.SCRAPER_DELAY_MIN, Config.SCRAPER_DELAY_MAX))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            logger.debug(f"✅ Fetched: {url}")
            return soup
        except Exception as e:
            logger.warning(f"⚠️ Timeout or error on {url}: {e}")
            try:
                return BeautifulSoup(self.driver.page_source, "lxml")
            except:
                return None

    def parse_jobs(self, soup) -> list:
        jobs = []
        if not soup:
            return jobs

        job_cards = soup.find_all("div", class_="srp-jobtuple-wrapper")
        logger.info(f"Found {len(job_cards)} job cards")

        for card in job_cards:
            try:
                title_tag = card.find("a", class_="title")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                url   = title_tag.get("href", "")

                if job_exists(url):
                    logger.info(f"Skipping duplicate: {title}")
                    continue

                company_tag  = card.find("a", class_="comp-name")
                company      = company_tag.get_text(strip=True) if company_tag else None

                location_tag = card.find("span", class_="locWdth")
                location     = location_tag.get_text(strip=True) if location_tag else None

                salary_tag   = card.find("span", class_="sal")
                salary_raw   = salary_tag.get_text(strip=True) if salary_tag else "Not disclosed"

                exp_tag      = card.find("span", class_="expwdth")
                experience   = exp_tag.get_text(strip=True) if exp_tag else None

                desc_tag     = card.find("span", class_="job-desc")
                description  = desc_tag.get_text(strip=True) if desc_tag else ""

                jobs.append({
                    "source":      "naukri",
                    "title":       title,
                    "company":     company,
                    "location":    location,
                    "salary_raw":  salary_raw,
                    "experience":  experience,
                    "description": description,
                    "url":         url,
                })

            except Exception as e:
                logger.warning(f"Error parsing card: {e}")
                continue

        return jobs

    def scrape(self):
        total_inserted = 0
        try:
            for keyword in SEARCH_KEYWORDS:
                for location in LOCATIONS:
                    for page in range(1, self.pages_per_location + 1):
                        if page == 1:
                            url = f"https://www.naukri.com/{keyword}-jobs-in-{location}"
                        else:
                            url = f"https://www.naukri.com/{keyword}-jobs-in-{location}-{page}"

                        logger.info(f"🔍 Scraping: {keyword} | {location} | page {page}")
                        soup = self.get_page_source(url)
                        jobs = self.parse_jobs(soup)

                        for job in jobs:
                            try:
                                insert_raw_job(job)
                                total_inserted += 1
                            except Exception as e:
                                logger.warning(f"⚠️ Insert failed: {e}")

                        time.sleep(random.uniform(Config.SCRAPER_DELAY_MIN, Config.SCRAPER_DELAY_MAX))
        except Exception as e:
            logger.error(f"❌ Scraping error: {e}")
        finally:
            try:
                self.driver.quit()
                logger.info("✅ Browser closed")
            except:
                pass
        
        logger.info(f"✅ Naukri scraping done. Total inserted: {total_inserted}")
        return total_inserted


if __name__ == "__main__":
    scraper = NaukriScraper()
    scraper.scrape()