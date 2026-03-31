import requests
from bs4 import BeautifulSoup
from loguru import logger
import time
import random

class BaseScraper:
    def __init__(self, source: str):
        self.source  = source
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    def get_page(self, url: str) -> BeautifulSoup | None:
        try:
            time.sleep(random.uniform(2, 5))
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            logger.info(f"Fetched: {url}")
            return BeautifulSoup(response.text, "lxml")
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def scrape(self):
        raise NotImplementedError("Each scraper must implement scrape()")