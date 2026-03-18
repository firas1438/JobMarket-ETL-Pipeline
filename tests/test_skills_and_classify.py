from __future__ import annotations

from app.utils.skills import enrich_job


def test_enrich_job_extracts_skills():
    enr = enrich_job(
        "Data Engineer",
        "We use Python, SQL, Docker and Kafka. Dashboarding with Tableau.",
    )
    assert "python" in enr.skills
    assert "sql" in enr.skills
    assert "docker" in enr.skills
    assert "kafka" in enr.skills
    assert "tableau" in enr.skills


def test_enrich_job_classifies_role_and_seniority():
    enr = enrich_job(
        "Senior Backend Engineer",
        "Build API services in Python and PostgreSQL.",
    )
    assert enr.role_type in {"Backend", "Other"}
    assert enr.seniority_level == "Senior"

