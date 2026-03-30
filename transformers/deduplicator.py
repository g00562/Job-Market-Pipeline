from loguru import logger
import psycopg2
from loaders.postgres_loader import get_connection

def get_duplicate_urls() -> list:
    """Find jobs posted on both Naukri and Indeed by matching title + company."""
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT a.id, b.id
            FROM jobs_raw a
            JOIN jobs_raw b
                ON LOWER(TRIM(a.title))   = LOWER(TRIM(b.title))
                AND LOWER(TRIM(a.company)) = LOWER(TRIM(b.company))
                AND a.source != b.source
                AND a.id < b.id
        """)
        duplicates = cur.fetchall()
        logger.info(f"Found {len(duplicates)} cross-platform duplicate pairs")
        return duplicates
    finally:
        cur.close()
        conn.close()

def mark_duplicates():
    """Log duplicate pairs — we keep both but flag them."""
    duplicates = get_duplicate_urls()
    if not duplicates:
        logger.info("No duplicates found across platforms")
        return 0

    for naukri_id, indeed_id in duplicates:
        logger.info(f"Duplicate pair — Naukri id:{naukri_id} | Indeed id:{indeed_id}")

    logger.info(f"Total duplicate pairs: {len(duplicates)}")
    return len(duplicates)

def get_all_raw_jobs() -> list:
    """Fetch all raw jobs for transformation."""
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT id, source, title, company, location,
                   salary_raw, experience, description, url
            FROM jobs_raw
            WHERE id NOT IN (SELECT raw_id FROM jobs_cleaned WHERE raw_id IS NOT NULL)
            ORDER BY id
        """)
        cols = ["id","source","title","company","location",
                "salary_raw","experience","description","url"]
        rows = cur.fetchall()
        jobs = [dict(zip(cols, row)) for row in rows]
        logger.info(f"Fetched {len(jobs)} raw jobs for transformation")
        return jobs
    finally:
        cur.close()
        conn.close()