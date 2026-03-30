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
    url         VARCHAR(500),
    scraped_at  TIMESTAMP DEFAULT NOW()
);

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
    posted_at      TIMESTAMP DEFAULT NOW()
);

-- Skills extracted table
CREATE TABLE IF NOT EXISTS skills_extracted (
    id         SERIAL PRIMARY KEY,
    job_id     INT REFERENCES jobs_cleaned(id),
    skill      VARCHAR(100),
    source     VARCHAR(20)
);

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id        SERIAL PRIMARY KEY,
    name      VARCHAR(255) UNIQUE,
    city      VARCHAR(100),
    job_count INT DEFAULT 1
);