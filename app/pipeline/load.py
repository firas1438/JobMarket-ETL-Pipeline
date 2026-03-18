from __future__ import annotations

import pandas as pd
from sqlalchemy import Engine, text

from app.config.db import get_engine
from app.utils.logger import get_logger


logger = get_logger("pipeline.load")


def truncate_and_load_staging(engine: Engine, df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs_staging;"))
    if df.empty:
        return
    df.to_sql("jobs_staging", engine, if_exists="append", index=False, method="multi", chunksize=1000)


UPSERT_JOBS_CLEAN = """
INSERT INTO jobs_clean (
  job_hash, job_id, source, title, company, location, is_remote, category,
  publication_date, job_url, description, skills_extracted, role_type, seniority_level, ingested_at
)
SELECT
  job_hash, job_id, source, title, company, location, is_remote, category,
  publication_date, job_url, description, skills_extracted, role_type, seniority_level, ingested_at
FROM jobs_staging
ON CONFLICT (job_hash) DO UPDATE SET
  job_id = EXCLUDED.job_id,
  source = EXCLUDED.source,
  title = EXCLUDED.title,
  company = EXCLUDED.company,
  location = EXCLUDED.location,
  is_remote = EXCLUDED.is_remote,
  category = EXCLUDED.category,
  publication_date = EXCLUDED.publication_date,
  job_url = COALESCE(EXCLUDED.job_url, jobs_clean.job_url),
  description = EXCLUDED.description,
  skills_extracted = EXCLUDED.skills_extracted,
  role_type = EXCLUDED.role_type,
  seniority_level = EXCLUDED.seniority_level,
  ingested_at = EXCLUDED.ingested_at;
"""


def upsert_clean(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(UPSERT_JOBS_CLEAN))


METRICS_SQL = """
WITH dates AS (
  SELECT DISTINCT publication_date::date AS metric_date
  FROM jobs_clean
  WHERE publication_date IS NOT NULL
),
totals AS (
  SELECT publication_date::date AS metric_date, COUNT(*) AS total_jobs
  FROM jobs_clean
  WHERE publication_date IS NOT NULL
  GROUP BY 1
),
company_counts AS (
  SELECT publication_date::date AS metric_date, company, COUNT(*) AS jobs
  FROM jobs_clean
  WHERE publication_date IS NOT NULL AND company IS NOT NULL AND company <> ''
  GROUP BY 1, 2
),
skill_counts AS (
  SELECT publication_date::date AS metric_date, skill, COUNT(*) AS jobs
  FROM jobs_clean, UNNEST(skills_extracted) AS skill
  WHERE publication_date IS NOT NULL
  GROUP BY 1, 2
),
role_counts AS (
  SELECT publication_date::date AS metric_date, COALESCE(role_type, 'Unspecified') AS role_type, COUNT(*) AS jobs
  FROM jobs_clean
  WHERE publication_date IS NOT NULL
  GROUP BY 1, 2
),
assembled AS (
  SELECT
    d.metric_date,
    COALESCE(t.total_jobs, 0) AS total_jobs,
    COALESCE(
      (
        SELECT jsonb_agg(jsonb_build_object('company', cc.company, 'jobs', cc.jobs) ORDER BY cc.jobs DESC)
        FROM (
          SELECT company, jobs
          FROM company_counts
          WHERE metric_date = d.metric_date
          ORDER BY jobs DESC
          LIMIT 10
        ) cc
      ),
      '[]'::jsonb
    ) AS top_companies,
    COALESCE(
      (
        SELECT jsonb_agg(jsonb_build_object('skill', sc.skill, 'jobs', sc.jobs) ORDER BY sc.jobs DESC)
        FROM (
          SELECT skill, jobs
          FROM skill_counts
          WHERE metric_date = d.metric_date
          ORDER BY jobs DESC
          LIMIT 10
        ) sc
      ),
      '[]'::jsonb
    ) AS top_skills,
    COALESCE(
      (
        SELECT jsonb_object_agg(rc.role_type, rc.jobs)
        FROM role_counts rc
        WHERE rc.metric_date = d.metric_date
      ),
      '{}'::jsonb
    ) AS role_counts
  FROM dates d
  LEFT JOIN totals t ON t.metric_date = d.metric_date
)
INSERT INTO daily_metrics (metric_date, total_jobs, top_companies, top_skills, role_counts, computed_at)
SELECT metric_date, total_jobs, top_companies, top_skills, role_counts, NOW()
FROM assembled
ON CONFLICT (metric_date) DO UPDATE SET
  total_jobs = EXCLUDED.total_jobs,
  top_companies = EXCLUDED.top_companies,
  top_skills = EXCLUDED.top_skills,
  role_counts = EXCLUDED.role_counts,
  computed_at = EXCLUDED.computed_at;
"""


def recompute_daily_metrics(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(METRICS_SQL))


def run_load(df: pd.DataFrame) -> None:
    engine = get_engine()
    logger.info("loading_staging", extra={"extra": {"rows": int(df.shape[0])}})
    truncate_and_load_staging(engine, df)
    logger.info("upserting_jobs_clean")
    upsert_clean(engine)
    logger.info("recomputing_daily_metrics")
    recompute_daily_metrics(engine)
    logger.info("batch_load_done")

