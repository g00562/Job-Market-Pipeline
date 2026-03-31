import pandas as pd
from loaders.postgres_loader import get_connection
import os

conn = get_connection()

# Export 1 — Jobs cleaned
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
""", conn)

# Export 2 — Top skills
df_skills = pd.read_sql("""
    SELECT skill, source, COUNT(*) as job_count
    FROM skills_extracted
    GROUP BY skill, source
    ORDER BY job_count DESC
""", conn)

# Export 3 — Jobs by city
df_cities = pd.read_sql("""
    SELECT city, source, COUNT(*) as job_count
    FROM jobs_cleaned
    WHERE city IS NOT NULL
    GROUP BY city, source
    ORDER BY job_count DESC
""", conn)

# Export 4 — Salary by city
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
""", conn)

# Export 5 — Top companies
df_companies = pd.read_sql("""
    SELECT company, city, source, COUNT(*) as job_count
    FROM jobs_cleaned
    WHERE company IS NOT NULL
    GROUP BY company, city, source
    ORDER BY job_count DESC
    LIMIT 50
""", conn)

# Export 6 — Experience distribution
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
""", conn)

conn.close()

# Save all to CSV
os.makedirs("exports", exist_ok=True)
df_jobs.to_csv("exports/jobs_cleaned.csv", index=False)
df_skills.to_csv("exports/top_skills.csv", index=False)
df_cities.to_csv("exports/jobs_by_city.csv", index=False)
df_salary.to_csv("exports/salary_by_city.csv", index=False)
df_companies.to_csv("exports/top_companies.csv", index=False)
df_exp.to_csv("exports/experience_distribution.csv", index=False)

print("✅ All CSVs exported to /exports folder!")
print(f"  jobs_cleaned.csv       → {len(df_jobs)} rows")
print(f"  top_skills.csv         → {len(df_skills)} rows")
print(f"  jobs_by_city.csv       → {len(df_cities)} rows")
print(f"  salary_by_city.csv     → {len(df_salary)} rows")
print(f"  top_companies.csv      → {len(df_companies)} rows")
print(f"  experience_distribution.csv → {len(df_exp)} rows")