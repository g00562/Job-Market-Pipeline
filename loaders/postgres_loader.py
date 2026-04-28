import psycopg2
from psycopg2 import pool, Error, DatabaseError
from pathlib import Path
import time
from loguru import logger
import sys

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import Config

# Connection pool (singleton)
_connection_pool = None

def get_connection_pool():
    """Get or create connection pool"""
    global _connection_pool
    if _connection_pool is None:
        try:
            _connection_pool = psycopg2.pool.SimpleConnectionPool(
                1,
                Config.DB_POOL_SIZE,
                **Config.get_psycopg2_params()
            )
            logger.info(f"✅ Connection pool created (size: {Config.DB_POOL_SIZE})")
        except Error as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            raise
    return _connection_pool

def get_connection(max_retries=None):
    """Get connection from pool with retry logic"""
    if max_retries is None:
        max_retries = Config.MAX_RETRIES
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            conn_pool = get_connection_pool()
            conn = conn_pool.getconn()
            if conn:
                logger.debug("✅ Database connection acquired from pool")
                return conn
        except (DatabaseError, psycopg2.OperationalError) as e:
            retry_count += 1
            logger.warning(f"⚠️ Connection attempt {retry_count}/{max_retries} failed: {e}")
            if retry_count < max_retries:
                time.sleep(Config.RETRY_DELAY)
            else:
                logger.error(f"❌ Failed to connect after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"❌ Unexpected connection error: {e}")
            raise
    
    raise Exception("Failed to acquire database connection")

def return_connection(conn):
    """Return connection back to pool"""
    try:
        if conn:
            conn_pool = get_connection_pool()
            conn_pool.putconn(conn)
            logger.debug("✅ Connection returned to pool")
    except Exception as e:
        logger.warning(f"⚠️ Failed to return connection to pool: {e}")
        try:
            conn.close()
        except:
            pass

def close_connection_pool():
    """Close all connections in pool"""
    global _connection_pool
    if _connection_pool:
        try:
            _connection_pool.closeall()
            _connection_pool = None
            logger.info("✅ Connection pool closed")
        except Exception as e:
            logger.error(f"❌ Error closing connection pool: {e}")

def create_tables():
    """Create database tables from SQL schema"""
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_file = Config.SQL_DIR / "create_tables.sql"
        if not sql_file.exists():
            logger.error(f"❌ SQL file not found: {sql_file}")
            raise FileNotFoundError(f"SQL schema file not found: {sql_file}")
        
        with open(sql_file, "r") as f:
            sql = f.read()
        
        # Split and execute statements (PostgreSQL doesn't like multiple statements at once)
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        for i, statement in enumerate(statements):
            try:
                cur.execute(statement)
                logger.debug(f"✅ Executed statement {i+1}/{len(statements)}")
            except psycopg2.Error as e:
                if "already exists" in str(e):
                    logger.info(f"ℹ️ Table already exists")
                else:
                    logger.error(f"❌ Error executing statement {i+1}: {e}")
                    raise
        
        conn.commit()
        logger.info("✅ Database tables created successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Table creation failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def insert_raw_job(job: dict) -> int:
    """Insert a raw job into the database with error handling"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO jobs_raw
                    (source, title, company, location, salary_raw,
                     experience, description, url)
                VALUES
                    (%(source)s, %(title)s, %(company)s, %(location)s,
                     %(salary_raw)s, %(experience)s, %(description)s, %(url)s)
                RETURNING id;
            """, job)
            job_id = cur.fetchone()[0]
            conn.commit()
            logger.debug(f"✅ Inserted raw job id={job_id} | {job.get('title', 'N/A')[:50]}")
            return job_id
        except psycopg2.IntegrityError as e:
            conn.rollback()
            logger.warning(f"⚠️ Duplicate or constraint violation: {e}")
            raise
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"❌ Database error inserting job: {e}")
            raise
    except Exception as e:
        logger.error(f"❌ Insert failed for job {job.get('title', 'N/A')}: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def insert_cleaned_job(job: dict) -> int:
    """Insert cleaned job with error handling"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO jobs_cleaned
                    (raw_id, title_std, company, city, is_remote,
                     salary_min, salary_max, experience_min,
                     experience_max, source)
                VALUES
                    (%(raw_id)s, %(title_std)s, %(company)s, %(city)s,
                     %(is_remote)s, %(salary_min)s, %(salary_max)s,
                     %(experience_min)s, %(experience_max)s, %(source)s)
                RETURNING id;
            """, job)
            job_id = cur.fetchone()[0]
            conn.commit()
            logger.debug(f"✅ Inserted cleaned job id={job_id}")
            return job_id
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"❌ Cleaned job insert failed: {e}")
            raise
    except Exception as e:
        logger.error(f"❌ Insert cleaned job failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def insert_skills(job_id: int, skills: list, source: str):
    """Insert skills with batch processing and error handling"""
    conn = None
    if not skills:
        logger.debug(f"ℹ️ No skills to insert for job_id={job_id}")
        return
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            # Batch insert for efficiency
            skill_tuples = [(job_id, skill, source) for skill in skills]
            cur.executemany("""
                INSERT INTO skills_extracted (job_id, skill, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (job_id, skill) DO NOTHING
            """, skill_tuples)
            conn.commit()
            logger.debug(f"✅ Inserted {len(skills)} skills for job_id={job_id}")
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"❌ Skills insert failed: {e}")
            raise
    except Exception as e:
        logger.error(f"❌ Insert skills failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def job_exists(url: str) -> bool:
    """Check if job URL already exists in database"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM jobs_raw WHERE url = %s LIMIT 1", (url,))
            exists = cur.fetchone() is not None
            logger.debug(f"✅ Job exists check: {exists}")
            return exists
        except psycopg2.Error as e:
            logger.error(f"❌ Error checking job existence: {e}")
            raise
    except Exception as e:
        logger.error(f"❌ Job exists check failed: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def mark_duplicates() -> int:
    """
    Detect and mark duplicate jobs across different sources.
    Returns count of duplicate pairs found.
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        try:
            # Find duplicates: same title + company + different sources
            cur.execute("""
                SELECT a.id, b.id
                FROM jobs_raw a
                JOIN jobs_raw b
                    ON LOWER(TRIM(a.title))    = LOWER(TRIM(b.title))
                    AND LOWER(TRIM(a.company)) = LOWER(TRIM(b.company))
                    AND a.source != b.source
                    AND a.id < b.id
                WHERE a.is_duplicate = FALSE
                AND b.is_duplicate = FALSE
            """)
            
            duplicates = cur.fetchall()
            
            if not duplicates:
                logger.info("✅ No duplicates found")
                return 0
            
            # Mark secondary job as duplicate (keep first occurrence)
            for id1, id2 in duplicates:
                cur.execute(
                    "UPDATE jobs_raw SET is_duplicate = TRUE WHERE id = %s",
                    (id2,)
                )
                logger.debug(f"🔗 Marked duplicate pair: id={id1} | id={id2}")
            
            conn.commit()
            logger.info(f"✅ Marked {len(duplicates)} duplicate pairs")
            return len(duplicates)
            
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"❌ Duplicate detection failed: {e}")
            raise
            
    except Exception as e:
        logger.error(f"❌ Mark duplicates failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)

def get_all_raw_jobs(exclude_duplicates: bool = True) -> list:
    """
    Fetch all raw jobs that haven't been cleaned yet.
    Filter by data engineer job titles only.
    
    Args:
        exclude_duplicates: If True, exclude jobs marked as duplicates
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        try:
            duplicate_filter = "AND is_duplicate = FALSE" if exclude_duplicates else ""
            
            cur.execute(f"""
                SELECT id, source, title, company, location,
                       salary_raw, experience, description, url
                FROM jobs_raw
                WHERE id NOT IN (
                    SELECT raw_id FROM jobs_cleaned WHERE raw_id IS NOT NULL
                )
                AND (
                    LOWER(title) LIKE '%data engineer%'
                    OR LOWER(title) LIKE '%data engineering%'
                    OR LOWER(title) LIKE '%etl developer%'
                    OR LOWER(title) LIKE '%etl engineer%'
                    OR LOWER(title) LIKE '%data pipeline%'
                    OR LOWER(title) LIKE '%big data%'
                    OR LOWER(title) LIKE '%data warehouse%'
                    OR LOWER(title) LIKE '%dwh engineer%'
                    OR LOWER(title) LIKE '%azure data%'
                    OR LOWER(title) LIKE '%aws data%'
                    OR LOWER(title) LIKE '%databricks%'
                    OR LOWER(title) LIKE '%analytics engineer%'
                    OR LOWER(title) LIKE '%data platform%'
                    OR LOWER(title) LIKE '%data integration%'
                    OR LOWER(title) LIKE '%gcp data%'
                )
                {duplicate_filter}
                ORDER BY id
            """)
            
            cols = ["id", "source", "title", "company", "location",
                    "salary_raw", "experience", "description", "url"]
            rows = cur.fetchall()
            
            result = [dict(zip(cols, row)) for row in rows]
            logger.info(f"✅ Fetched {len(result)} relevant raw jobs (excluding duplicates: {exclude_duplicates})")
            return result
            
        except psycopg2.Error as e:
            logger.error(f"❌ Failed to fetch raw jobs: {e}")
            raise
            
    except Exception as e:
        logger.error(f"❌ Get raw jobs failed: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            return_connection(conn)