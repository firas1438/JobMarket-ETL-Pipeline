from __future__ import annotations

import re
from dataclasses import dataclass


SKILL_KEYWORDS = [
    "python",
    "sql",
    "docker",
    "kafka",
    "airflow",
    "aws",
    "gcp",
    "azure",
    "power bi",
    "tableau",
    "spark",
    "dbt",
    "postgres",
    "postgresql",
]


ROLE_RULES: list[tuple[str, list[str]]] = [
    ("Data", ["data engineer", "data engineering", "etl", "analytics engineer", "data platform"]),
    ("ML", ["machine learning", "ml engineer", "data scientist", "llm", "nlp"]),
    ("Analyst", ["data analyst", "bi analyst", "business intelligence", "analytics", "reporting"]),
    ("Backend", ["backend", "api", "microservice", "server-side"]),
]


SENIORITY_RULES: list[tuple[str, list[str]]] = [
    ("Junior", ["junior", "entry", "intern", "graduate"]),
    ("Senior", ["senior", "staff", "principal", "lead", "architect"]),
    ("Mid", ["mid", "intermediate"]),
]


@dataclass(frozen=True)
class Enrichment:
    skills: list[str]
    role_type: str
    seniority_level: str


def extract_skills(text: str) -> list[str]:
    hay = text.lower()
    found: list[str] = []
    for kw in SKILL_KEYWORDS:
        # word-boundary-ish match for single tokens; allow spaces in keyword (power bi)
        pattern = r"\b" + re.escape(kw) + r"\b"
        if re.search(pattern, hay):
            found.append(kw)
    return sorted(set(found))


def classify_role(title: str, description: str) -> str:
    hay = f"{title}\n{description}".lower()
    for role, needles in ROLE_RULES:
        if any(n in hay for n in needles):
            return role
    return "Other"


def classify_seniority(title: str, description: str) -> str:
    hay = f"{title}\n{description}".lower()
    for level, needles in SENIORITY_RULES:
        if any(re.search(r"\b" + re.escape(n) + r"\b", hay) for n in needles):
            return level
    return "Unspecified"


def enrich_job(title: str, description: str) -> Enrichment:
    text = f"{title}\n{description}".strip()
    return Enrichment(
        skills=extract_skills(text),
        role_type=classify_role(title, description),
        seniority_level=classify_seniority(title, description),
    )

