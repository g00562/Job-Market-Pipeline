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

BASE_URL = "https://www.naukri.com/{keyword}-jobs-in-{location}-{page}"

class NaukriScraper:
    def __init__(self):
        self.source = "naukri"
        self.driver = self._init_driver()

    def _init_driver(self):
        options = Options()
        options.add_argument("--headless")           # Run without opening browser window
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
        logger.info("Chrome driver initialized")
        return driver

    def get_page_source(self, url: str) -> BeautifulSoup | None:
        try:
            self.driver.get(url)

            # Wait up to 15 seconds for job cards to appear
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "srp-jobtuple-wrapper"))
            )

            time.sleep(random.uniform(2, 4))  # Extra wait for full load
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            logger.info(f"Fetched: {url}")
            return soup

        except Exception as e:
            logger.warning(f"Timeout or error on {url}: {e}")
            # Still try to parse whatever loaded
            try:
                soup = BeautifulSoup(self.driver.page_source, "lxml")
                return soup
            except:
                return None

    def parse_jobs(self, soup) -> list:
        jobs = []
        if not soup:
            return jobs

        # Naukri's current job card class (2025-2026)
        job_cards = soup.find_all("div", class_="srp-jobtuple-wrapper")
        logger.info(f"Found {len(job_cards)} job cards")

        for card in job_cards:
            try:
                # Title + URL
                title_tag = card.find("a", class_="title")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                url   = title_tag.get("href", "")

                # Skip duplicates already in DB
                if job_exists(url):
                    logger.info(f"Skipping duplicate: {title}")
                    continue

                # Company
                company_tag = card.find("a", class_="comp-name")
                company = company_tag.get_text(strip=True) if company_tag else None

                # Location
                location_tag = card.find("span", class_="locWdth")
                if not location_tag:
                    location_tag = card.find("li", attrs={"type": "location"})
                location = location_tag.get_text(strip=True) if location_tag else None

                # Salary
                salary_tag = card.find("span", class_="sal")
                if not salary_tag:
                    salary_tag = card.find("li", attrs={"type": "salary"})
                salary_raw = salary_tag.get_text(strip=True) if salary_tag else "Not disclosed"

                # Experience
                exp_tag = card.find("span", class_="expwdth")
                if not exp_tag:
                    exp_tag = card.find("li", attrs={"type": "experience"})
                experience = exp_tag.get_text(strip=True) if exp_tag else None

                # Description
                desc_tag = card.find("span", class_="job-desc")
                description = desc_tag.get_text(strip=True) if desc_tag else ""

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
                    for page in range(1, 4):
                        # Naukri URL format for page 1 is different from rest
                        if page == 1:
                            url = f"https://www.naukri.com/{keyword}-jobs-in-{location}"
                        else:
                            url = f"https://www.naukri.com/{keyword}-jobs-in-{location}-{page}"

                        logger.info(f"Scraping: {keyword} | {location} | page {page}")
                        soup  = self.get_page_source(url)
                        jobs  = self.parse_jobs(soup)

                        for job in jobs:
                            try:
                                insert_raw_job(job)
                                total_inserted += 1
                            except Exception as e:
                                logger.error(f"Insert failed: {e}")

                        time.sleep(random.uniform(3, 5))

        finally:
            self.driver.quit()
            logger.info("Browser closed")

        logger.info(f"Naukri scraping done. Total inserted: {total_inserted}")
        return total_inserted


if __name__ == "__main__":
    scraper = NaukriScraper()
    scraper.scrape()