-- Core schema for Job Market Intelligence Pipeline

CREATE TABLE IF NOT EXISTS jobs_staging (
  job_hash TEXT NOT NULL,
  job_id TEXT,
  source TEXT NOT NULL,
  title TEXT,
  company TEXT,
  location TEXT,
  is_remote BOOLEAN,
  category TEXT,
  publication_date TIMESTAMPTZ,
  job_url TEXT,
  description TEXT,
  skills_extracted TEXT[],
  role_type TEXT,
  seniority_level TEXT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs_clean (
  job_hash TEXT PRIMARY KEY,
  job_id TEXT,
  source TEXT NOT NULL,
  title TEXT,
  company TEXT,
  location TEXT,
  is_remote BOOLEAN,
  category TEXT,
  publication_date TIMESTAMPTZ,
  job_url TEXT,
  description TEXT,
  skills_extracted TEXT[],
  role_type TEXT,
  seniority_level TEXT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Help enforce dedup when URL is present
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_clean_job_url
  ON jobs_clean (job_url)
  WHERE job_url IS NOT NULL AND job_url <> '';

CREATE TABLE IF NOT EXISTS jobs_stream (
  stream_id BIGSERIAL PRIMARY KEY,
  job_hash TEXT NOT NULL,
  job_id TEXT,
  source TEXT NOT NULL DEFAULT 'api',
  title TEXT,
  company TEXT,
  location TEXT,
  is_remote BOOLEAN,
  category TEXT,
  publication_date TIMESTAMPTZ,
  job_url TEXT,
  description TEXT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_stream_job_hash ON jobs_stream (job_hash);

CREATE TABLE IF NOT EXISTS daily_metrics (
  metric_date DATE PRIMARY KEY,
  total_jobs BIGINT NOT NULL,
  top_companies JSONB NOT NULL,
  top_skills JSONB NOT NULL,
  role_counts JSONB NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

