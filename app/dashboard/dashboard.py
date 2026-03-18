from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from app.config.db import get_engine
from app.utils.logger import get_logger


logger = get_logger("dashboard")


st.set_page_config(page_title="Job Market Intelligence", layout="wide")
st.title("Job Market Intelligence Dashboard")

engine = get_engine()


def qdf(sql: str) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)


col1, col2, col3 = st.columns(3)

try:
    total_jobs = int(qdf("SELECT COUNT(*) AS c FROM jobs_clean;")["c"].iloc[0])
    total_stream = int(qdf("SELECT COUNT(*) AS c FROM jobs_stream;")["c"].iloc[0])
    days = int(qdf("SELECT COUNT(*) AS c FROM daily_metrics;")["c"].iloc[0])
except Exception:
    logger.exception("dashboard_query_failed")
    st.error("Database not ready yet. Run `docker compose run --rm batch` to load initial data.")
    st.stop()

col1.metric("Total jobs (clean)", total_jobs)
col2.metric("Streaming jobs (jobs_stream)", total_stream)
col3.metric("Days with metrics", days)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Jobs over time")
    ts = qdf(
        """
        SELECT publication_date::date AS day, COUNT(*) AS jobs
        FROM jobs_clean
        WHERE publication_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """
    )
    if ts.empty:
        st.info("No publication dates available yet.")
    else:
        st.line_chart(ts.set_index("day")["jobs"])

with right:
    st.subheader("Jobs by role type")
    roles = qdf(
        """
        SELECT COALESCE(role_type, 'Unspecified') AS role_type, COUNT(*) AS jobs
        FROM jobs_clean
        GROUP BY 1
        ORDER BY jobs DESC;
        """
    )
    st.bar_chart(roles.set_index("role_type")["jobs"])

st.divider()

left2, right2 = st.columns(2)

with left2:
    st.subheader("Top companies")
    top_companies = qdf(
        """
        SELECT company, COUNT(*) AS jobs
        FROM jobs_clean
        WHERE company IS NOT NULL AND company <> ''
        GROUP BY 1
        ORDER BY jobs DESC
        LIMIT 10;
        """
    )
    st.dataframe(top_companies, use_container_width=True, hide_index=True)

with right2:
    st.subheader("Top skills")
    top_skills = qdf(
        """
        SELECT skill, COUNT(*) AS jobs
        FROM jobs_clean, UNNEST(skills_extracted) AS skill
        GROUP BY 1
        ORDER BY jobs DESC
        LIMIT 10;
        """
    )
    st.dataframe(top_skills, use_container_width=True, hide_index=True)

st.divider()

st.subheader("Latest streaming jobs")
latest_stream = qdf(
    """
    SELECT ingested_at, title, company, location, job_url
    FROM jobs_stream
    ORDER BY ingested_at DESC
    LIMIT 20;
    """
)
st.dataframe(latest_stream, use_container_width=True, hide_index=True)
