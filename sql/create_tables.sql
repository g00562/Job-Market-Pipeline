-- Add is_duplicate column if upgrading from older schema
ALTER TABLE jobs_raw ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE;

-- Jobs raw table (stores exactly what we scraped)
CREATE TABLE IF NOT EXISTS jobs_raw (
    id          SERIAL PRIMARY KEY,
    source      VARCHAR(20) NOT NULL,
    title       VARCHAR(255),
    company     VARCHAR(255),
    location    VARCHAR(255),
    salary_raw  VARCHAR(255),
    experience  VARCHAR(100),
    description TEXT,
    url         VARCHAR(500) UNIQUE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    scraped_at  TIMESTAMP DEFAULT NOW(),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Add index on is_duplicate for faster filtering
CREATE INDEX IF NOT EXISTS idx_jobs_raw_duplicate ON jobs_raw(is_duplicate);
CREATE INDEX IF NOT EXISTS idx_jobs_raw_source ON jobs_raw(source);
CREATE INDEX IF NOT EXISTS idx_jobs_raw_title ON jobs_raw(title);

-- Jobs cleaned table (transformed data)
CREATE TABLE IF NOT EXISTS jobs_cleaned (
    id             SERIAL PRIMARY KEY,
    raw_id         INT REFERENCES jobs_raw(id),
    title_std      VARCHAR(255),
    company        VARCHAR(255),
    city           VARCHAR(100),
    is_remote      BOOLEAN DEFAULT FALSE,
    salary_min     BIGINT,
    salary_max     BIGINT,
    experience_min INT,
    experience_max INT,
    source         VARCHAR(20),
    posted_at      TIMESTAMP DEFAULT NOW(),
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_cleaned_raw_id ON jobs_cleaned(raw_id);
CREATE INDEX IF NOT EXISTS idx_jobs_cleaned_source ON jobs_cleaned(source);
CREATE INDEX IF NOT EXISTS idx_jobs_cleaned_city ON jobs_cleaned(city);

-- Skills extracted table
CREATE TABLE IF NOT EXISTS skills_extracted (
    id         SERIAL PRIMARY KEY,
    job_id     INT REFERENCES jobs_cleaned(id),
    skill      VARCHAR(100),
    source     VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(job_id, skill)
);

CREATE INDEX IF NOT EXISTS idx_skills_job_id ON skills_extracted(job_id);
CREATE INDEX IF NOT EXISTS idx_skills_skill ON skills_extracted(skill);

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(255) UNIQUE,
    city       VARCHAR(100),
    job_count  INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);