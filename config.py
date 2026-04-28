"""
Configuration module for Job Market Pipeline
Handles environment variables with validation and defaults
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Load .env file from project root
ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(ENV_FILE)


class Config:
    """Configuration class with validation"""

    # Database Configuration
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "job_market_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    # Connection pooling
    DB_POOL_SIZE = int(os.getenv("CONNECTION_POOL_SIZE", "5"))
    DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", "30"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY_SECONDS", "5"))

    # Google Sheets Configuration
    GOOGLE_SHEETS_CREDS = os.getenv("GOOGLE_SHEETS_CREDS", "credentials/google_sheets_creds.json")
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

    # API Keys
    RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

    # Application Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    PROJECT_ROOT = Path(__file__).parent
    SQL_DIR = PROJECT_ROOT / "sql"
    EXPORTS_DIR = PROJECT_ROOT / "exports"
    CREDENTIALS_DIR = PROJECT_ROOT / "credentials"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Scraper Configuration
    SCRAPER_TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT", "30"))
    SCRAPER_DELAY_MIN = float(os.getenv("SCRAPER_DELAY_MIN", "2"))
    SCRAPER_DELAY_MAX = float(os.getenv("SCRAPER_DELAY_MAX", "5"))
    NAUKRI_PAGES = int(os.getenv("NAUKRI_PAGES_PER_LOCATION", "3"))
    LINKEDIN_PAGES = int(os.getenv("LINKEDIN_PAGES_PER_LOCATION", "3"))
    INDEED_PAGES = int(os.getenv("INDEED_PAGES_PER_LOCATION", "2"))

    @staticmethod
    def validate():
        """Validate all required configuration"""
        logger.info("🔍 Validating configuration...")
        
        errors = []

        # Check database credentials
        if not Config.DB_PASSWORD:
            errors.append("❌ DB_PASSWORD not set in .env file")

        if not Config.GOOGLE_SHEET_ID:
            logger.warning("⚠️ GOOGLE_SHEET_ID not set - Google Sheets export will fail")

        if not Config.RAPIDAPI_KEY:
            logger.warning("⚠️ RAPIDAPI_KEY not set - Indeed scraper will fail")

        # Check directories exist
        Config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        Config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        Config.CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)

        # Check SQL directory exists
        if not Config.SQL_DIR.exists():
            errors.append(f"❌ SQL directory not found: {Config.SQL_DIR}")

        # Check credentials file exists
        if not Path(Config.GOOGLE_SHEETS_CREDS).exists():
            logger.warning(f"⚠️ Google Sheets credentials not found: {Config.GOOGLE_SHEETS_CREDS}")

        if errors:
            for error in errors:
                logger.error(error)
            raise ValueError("Configuration validation failed. See errors above.")

        logger.info("✅ Configuration validation passed!")

    @staticmethod
    def get_db_url():
        """Get database URL for SQLAlchemy"""
        return (
            f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}"
            f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
        )

    @staticmethod
    def get_psycopg2_params():
        """Get psycopg2 connection parameters"""
        return {
            "host": Config.DB_HOST,
            "port": Config.DB_PORT,
            "dbname": Config.DB_NAME,
            "user": Config.DB_USER,
            "password": Config.DB_PASSWORD,
            "connect_timeout": Config.DB_TIMEOUT,
        }


# Validate configuration on import
try:
    Config.validate()
except Exception as e:
    logger.error(f"Configuration error: {e}")
    raise
