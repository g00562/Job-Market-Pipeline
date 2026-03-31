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
    "data engineer",
    "data engineering",
    "etl developer",
]

LOCATIONS = [
    "Bangalore",
    "Hyderabad",
    "Pune",
    "Mumbai",
    "Chennai",
]

class IndeedScraper:
    def __init__(self):
        self.source = "indeed"
        self.driver = self._init_driver()

    def _init_driver(self):
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
        driver  = webdriver.Chrome(service=service, options=options)
        logger.info("Indeed Chrome driver initialized")
        return driver

    def get_page_source(self, url: str) -> BeautifulSoup | None:
        try:
            self.driver.get(url)
            # Wait for job cards to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "job_seen_beacon"))
            )
            time.sleep(random.uniform(2, 4))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            logger.info(f"Fetched: {url}")
            return soup
        except Exception as e:
            logger.warning(f"Timeout or error on {url}: {e}")
            try:
                return BeautifulSoup(self.driver.page_source, "lxml")
            except:
                return None

    def parse_jobs(self, soup) -> list:
        jobs = []
        if not soup:
            return jobs

        # Indeed job cards
        job_cards = soup.find_all("div", class_="job_seen_beacon")
        logger.info(f"Found {len(job_cards)} Indeed job cards")

        for card in job_cards:
            try:
                # Title + URL
                title_tag = card.find("h2", class_="jobTitle")
                if not title_tag:
                    continue
                title     = title_tag.get_text(strip=True)
                link_tag  = title_tag.find("a")
                url       = "https://in.indeed.com" + link_tag.get("href", "") if link_tag else ""

                # Skip duplicates
                if url and job_exists(url):
                    logger.info(f"Skipping duplicate: {title}")
                    continue

                # Company
                company_tag = card.find("span", attrs={"data-testid": "company-name"})
                if not company_tag:
                    company_tag = card.find("span", class_="css-1h7lukg")
                company = company_tag.get_text(strip=True) if company_tag else None

                # Location
                location_tag = card.find("div", attrs={"data-testid": "text-location"})
                if not location_tag:
                    location_tag = card.find("div", class_="css-1restlb")
                location = location_tag.get_text(strip=True) if location_tag else None

                # Salary
                salary_tag = card.find("div", attrs={"data-testid": "attribute_snippet_testid"})
                if not salary_tag:
                    salary_tag = card.find("div", class_="salary-snippet-container")
                salary_raw = salary_tag.get_text(strip=True) if salary_tag else "Not disclosed"

                # Description snippet
                desc_tag = card.find("div", class_="job-snippet")
                if not desc_tag:
                    desc_tag = card.find("ul", class_="css-1lyr5hv")
                description = desc_tag.get_text(strip=True) if desc_tag else ""

                jobs.append({
                    "source":      "indeed",
                    "title":       title,
                    "company":     company,
                    "location":    location,
                    "salary_raw":  salary_raw,
                    "experience":  None,  # Indeed doesn't always show experience
                    "description": description,
                    "url":         url,
                })

            except Exception as e:
                logger.warning(f"Error parsing Indeed card: {e}")
                continue

        return jobs

    def scrape(self):
        total_inserted = 0

        try:
            for keyword in SEARCH_KEYWORDS:
                for location in LOCATIONS:
                    for page in range(0, 3):
                        # Indeed pagination uses start=0, 10, 20
                        start = page * 10
                        url   = (
                            f"https://in.indeed.com/jobs?"
                            f"q={keyword.replace(' ', '+')}"
                            f"&l={location}"
                            f"&start={start}"
                        )

                        logger.info(f"Scraping Indeed: {keyword} | {location} | page {page+1}")
                        soup = self.get_page_source(url)
                        jobs = self.parse_jobs(soup)

                        for job in jobs:
                            try:
                                insert_raw_job(job)
                                total_inserted += 1
                            except Exception as e:
                                logger.error(f"Insert failed: {e}")

                        time.sleep(random.uniform(3, 6))

        finally:
            self.driver.quit()
            logger.info("Indeed browser closed")

        logger.info(f"Indeed scraping done. Total inserted: {total_inserted}")
        return total_inserted


if __name__ == "__main__":
    scraper = IndeedScraper()
    scraper.scrape()