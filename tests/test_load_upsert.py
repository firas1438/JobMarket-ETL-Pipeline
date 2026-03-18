from __future__ import annotations

from app.pipeline import load


def test_upsert_sql_contains_on_conflict():
    sql = load.UPSERT_JOBS_CLEAN
    assert "ON CONFLICT" in sql
    assert "INSERT INTO jobs_clean" in sql


def test_metrics_sql_is_defined():
    assert isinstance(load.METRICS_SQL, str)
    assert "daily_metrics" in load.METRICS_SQL

