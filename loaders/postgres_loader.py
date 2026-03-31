import psycopg2
from dotenv import load_dotenv
import os
from loguru import logger

load_dotenv()

def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        logger.info("Connected to PostgreSQL successfully")
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise

def create_tables():
    conn = get_connection()
    cur  = conn.cursor()
    try:
        with open("sql/create_tables.sql", "r") as f:
            sql = f.read()
        cur.execute(sql)
        conn.commit()
        logger.info("Tables created successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Table creation failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def insert_raw_job(job: dict) -> int:
    conn = get_connection()
    cur  = conn.cursor()
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
        logger.info(f"Inserted raw job id={job_id} | {job['title']} @ {job['company']}")
        return job_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Insert failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def insert_cleaned_job(job: dict) -> int:
    conn = get_connection()
    cur  = conn.cursor()
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
        logger.info(f"Inserted cleaned job id={job_id}")
        return job_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Cleaned insert failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def insert_skills(job_id: int, skills: list, source: str):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        for skill in skills:
            cur.execute("""
                INSERT INTO skills_extracted (job_id, skill, source)
                VALUES (%s, %s, %s)
            """, (job_id, skill, source))
        conn.commit()
        logger.info(f"Inserted {len(skills)} skills for job_id={job_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Skills insert failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def job_exists(url: str) -> bool:
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT id FROM jobs_raw WHERE url = %s", (url,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()