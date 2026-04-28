import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
import os
import sys
from pathlib import Path
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))
from config import Config

# ============================================
# Google Sheets Configuration
# ============================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

CREDS_FILE = Config.GOOGLE_SHEETS_CREDS
SPREADSHEET_ID = Config.GOOGLE_SHEET_ID


# ============================================
# Database Connection (SQLAlchemy — No Warnings)
# ============================================
def get_sqlalchemy_engine():
    """Create SQLAlchemy engine using config"""
    try:
        engine = create_engine(
            Config.get_db_url(),
            pool_size=Config.DB_POOL_SIZE,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )
        logger.info("✅ SQLAlchemy engine created")
        return engine
    except Exception as e:
        logger.error(f"❌ Failed to create SQLAlchemy engine: {e}")
        raise


# ============================================
# Google Sheets Client
# ============================================
def get_sheets_client():
    """Authenticate and return Google Sheets client"""
    try:
        if not Path(CREDS_FILE).exists():
            logger.error(f"❌ Credentials file not found: {CREDS_FILE}")
            raise FileNotFoundError(f"Google Sheets credentials not found: {CREDS_FILE}")
        
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        logger.info(f"✅ Google Sheets client authenticated")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to authenticate Google Sheets: {e}")
        raise


# ============================================
# Upload DataFrame to Google Sheet Tab
# ============================================
def upload_to_sheet(client, spreadsheet_id, sheet_name, df):
    """Upload a DataFrame to a specific sheet tab"""
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)

        # Try to get existing sheet, or create new one
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()  # Clear old data
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=len(df) + 1,
                cols=len(df.columns)
            )

        # Handle NaN and None values — convert to empty string
        df_clean = df.fillna("")
        
        # Convert all values to strings (avoiding "None" string representation)
        def safe_str(val):
            if pd.isna(val) or val is None:
                return ""
            return str(val)
        
        # Apply safe conversion column by column
        df_converted = df_clean.map(safe_str)
        
        # Prepare data for upload
        data = [df_converted.columns.tolist()] + df_converted.values.tolist()

        # Upload data
        worksheet.update(range_name='A1', values=data)

        logger.info(f"✅ Uploaded '{sheet_name}' → {len(df)} rows")

    except Exception as e:
        logger.error(f"❌ Failed to upload '{sheet_name}': {e}")
        raise


# ============================================
# Fetch All Data from PostgreSQL
# ============================================
def fetch_data():
    """Fetch all data from PostgreSQL using SQLAlchemy"""
    engine = get_sqlalchemy_engine()

    # Export 1 — jobs_cleaned
    df_jobs = pd.read_sql("""
        SELECT
            title_std, company, city,
            is_remote,
            ROUND(salary_min / 100000.0, 1) as salary_min_lpa,
            ROUND(salary_max / 100000.0, 1) as salary_max_lpa,
            experience_min, experience_max,
            source, posted_at
        FROM jobs_cleaned
        WHERE title_std IS NOT NULL
    """, engine)

    # Export 2 — top_skills
    df_skills = pd.read_sql("""
        SELECT skill, source, COUNT(*) as job_count
        FROM skills_extracted
        GROUP BY skill, source
        ORDER BY job_count DESC
    """, engine)

    # Export 3 — jobs_by_city
    df_cities = pd.read_sql("""
        SELECT city, source, COUNT(*) as job_count
        FROM jobs_cleaned
        WHERE city IS NOT NULL
        GROUP BY city, source
        ORDER BY job_count DESC
    """, engine)

    # Export 4 — salary_by_city
    df_salary = pd.read_sql("""
        SELECT
            city,
            ROUND(AVG(salary_min)/100000.0, 1) as avg_min_lpa,
            ROUND(AVG(salary_max)/100000.0, 1) as avg_max_lpa,
            COUNT(*) as job_count
        FROM jobs_cleaned
        WHERE city IS NOT NULL
        AND salary_min IS NOT NULL
        GROUP BY city
        ORDER BY avg_max_lpa DESC
    """, engine)

    # Export 5 — top_companies
    df_companies = pd.read_sql("""
        SELECT company, city, source, COUNT(*) as job_count
        FROM jobs_cleaned
        WHERE company IS NOT NULL
        GROUP BY company, city, source
        ORDER BY job_count DESC
        LIMIT 50
    """, engine)

    # Export 6 — experience_distribution
    df_exp = pd.read_sql("""
        SELECT
            CASE
                WHEN experience_min = 0 THEN 'Fresher (0 yrs)'
                WHEN experience_min BETWEEN 1 AND 3 THEN 'Junior (1-3 yrs)'
                WHEN experience_min BETWEEN 4 AND 6 THEN 'Mid (4-6 yrs)'
                WHEN experience_min BETWEEN 7 AND 10 THEN 'Senior (7-10 yrs)'
                ELSE 'Expert (10+ yrs)'
            END as experience_level,
            COUNT(*) as job_count
        FROM jobs_cleaned
        WHERE experience_min IS NOT NULL
        GROUP BY experience_level
        ORDER BY job_count DESC
    """, engine)

    # Clean up connection
    engine.dispose()

    return {
        "jobs_cleaned": df_jobs,
        "top_skills": df_skills,
        "jobs_by_city": df_cities,
        "salary_by_city": df_salary,
        "top_companies": df_companies,
        "experience_distribution": df_exp
    }


# ============================================
# Export to Google Sheets (Main Function)
# ============================================
def export_to_google_sheets():
    """Fetch data from PostgreSQL and upload to Google Sheets"""
    try:
        logger.info("🚀 Starting Google Sheets export...")

        # Fetch data from PostgreSQL
        datasets = fetch_data()

        # Connect to Google Sheets
        client = get_sheets_client()

        # Upload each dataset as a separate tab/sheet
        for sheet_name, df in datasets.items():
            upload_to_sheet(client, SPREADSHEET_ID, sheet_name, df)

        logger.info("🎉 All data uploaded to Google Sheets successfully!")

        # Print summary
        print("\n" + "=" * 60)
        print("📊 GOOGLE SHEETS EXPORT SUMMARY")
        print("=" * 60)
        for sheet_name, df in datasets.items():
            print(f"  {sheet_name:30s} → {len(df):5d} rows")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Google Sheets export failed: {e}")
        raise


# ============================================
# Export to CSV (Backup)
# ============================================
def export_to_csv_backup():
    """Save CSV locally as backup"""
    try:
        datasets = fetch_data()
        Config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

        file_map = {
            "jobs_cleaned": "jobs_cleaned.csv",
            "top_skills": "top_skills.csv",
            "jobs_by_city": "jobs_by_city.csv",
            "salary_by_city": "salary_by_city.csv",
            "top_companies": "top_companies.csv",
            "experience_distribution": "experience_distribution.csv"
        }

        for sheet_name, df in datasets.items():
            filename = file_map[sheet_name]
            filepath = Config.EXPORTS_DIR / filename
            df.to_csv(filepath, index=False)
            logger.info(f"✅ Saved {filename}")

        logger.info(f"💾 CSV backup saved to {Config.EXPORTS_DIR}")
    except Exception as e:
        logger.error(f"❌ CSV export failed: {e}")
        raise


# ============================================
# Main Execution
# ============================================
if __name__ == "__main__":
    export_to_google_sheets()    # Upload to Google Sheets
    export_to_csv_backup()       # Also save CSV as backup