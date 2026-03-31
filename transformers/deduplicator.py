from loguru import logger
from loaders.postgres_loader import get_connection

def get_duplicate_urls() -> list:
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT a.id, b.id
            FROM jobs_raw a
            JOIN jobs_raw b
                ON LOWER(TRIM(a.title))    = LOWER(TRIM(b.title))
                AND LOWER(TRIM(a.company)) = LOWER(TRIM(b.company))
                AND a.source != b.source
                AND a.id < b.id
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def mark_duplicates():
    duplicates = get_duplicate_urls()
    if not duplicates:
        logger.info("No duplicates found")
        return 0
    for id1, id2 in duplicates:
        logger.info(f"Duplicate pair — id:{id1} | id:{id2}")
    logger.info(f"Total duplicate pairs: {len(duplicates)}")
    return len(duplicates)

def get_all_raw_jobs() -> list:
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT id, source, title, company, location,
                   salary_raw, experience, description, url
            FROM jobs_raw
            WHERE id NOT IN (
                SELECT raw_id FROM jobs_cleaned WHERE raw_id IS NOT NULL
            )
            ORDER BY id
        """)
        cols = ["id","source","title","company","location",
                "salary_raw","experience","description","url"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()