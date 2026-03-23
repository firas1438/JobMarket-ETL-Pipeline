from __future__ import annotations

from typing import Any

import pandas as pd

from app.utils.helpers import normalize_whitespace, parse_datetime, sha256_hex, utc_now
from app.utils.skills import enrich_job


UNIFIED_COLUMNS = [
    "job_hash",
    "job_id",
    "source",
    "title",
    "company",
    "location",
    "is_remote",
    "category",
    "publication_date",
    "job_url",
    "description",
    "skills_extracted",
    "role_type",
    "seniority_level",
    "ingested_at",
]


def normalize_remotive(raw_jobs: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for j in raw_jobs:
        job_id = str(j.get("id") or "") or None
        title = normalize_whitespace(j.get("title"))
        company = normalize_whitespace(j.get("company_name") or j.get("company"))
        location = normalize_whitespace(j.get("candidate_required_location") or j.get("location"))
        is_remote = True  # Remotive is remote-first
        category = normalize_whitespace(j.get("category"))
        pub_dt = parse_datetime(j.get("publication_date") or j.get("date"))
        url = normalize_whitespace(j.get("url") or j.get("job_url"))
        desc = normalize_whitespace(j.get("description") or "")

        base_for_hash = url or f"{title}|{company}|{pub_dt.isoformat() if pub_dt else ''}"
        job_hash = sha256_hex(base_for_hash)

        enrichment = enrich_job(title, desc)
        rows.append(
            {
                "job_hash": job_hash,
                "job_id": job_id,
                "source": "api",
                "title": title,
                "company": company,
                "location": location,
                "is_remote": bool(is_remote),
                "category": category,
                "publication_date": pub_dt,
                "job_url": url or None,
                "description": desc,
                "skills_extracted": enrichment.skills,
                "role_type": enrichment.role_type,
                "seniority_level": enrichment.seniority_level,
                "ingested_at": utc_now(),
            }
        )
    df = pd.DataFrame(rows)
    return ensure_columns(df)


def normalize_adzuna(raw_jobs: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for j in raw_jobs:
        job_id = str(j.get("id") or "") or None
        title = normalize_whitespace(j.get("title"))

        company_obj = j.get("company") or {}
        company = normalize_whitespace(
            company_obj.get("display_name") if isinstance(company_obj, dict) else j.get("company")
        )

        location_obj = j.get("location") or {}
        location = normalize_whitespace(
            location_obj.get("display_name") if isinstance(location_obj, dict) else j.get("location")
        )

        is_remote = "remote" in (location or "").lower()

        category_obj = j.get("category") or {}
        category = normalize_whitespace(
            category_obj.get("label") if isinstance(category_obj, dict) else j.get("category")
        )

        pub_dt = parse_datetime(j.get("created") or j.get("publication_date") or j.get("date"))
        url = normalize_whitespace(j.get("redirect_url") or j.get("url") or j.get("job_url"))
        desc = normalize_whitespace(j.get("description") or "")

        base_for_hash = url or f"{title}|{company}|{pub_dt.isoformat() if pub_dt else ''}"
        job_hash = sha256_hex(base_for_hash)

        enrichment = enrich_job(title, desc)
        rows.append(
            {
                "job_hash": job_hash,
                "job_id": job_id,
                "source": "api",
                "title": title,
                "company": company,
                "location": location,
                "is_remote": bool(is_remote),
                "category": category,
                "publication_date": pub_dt,
                "job_url": url or None,
                "description": desc,
                "skills_extracted": enrichment.skills,
                "role_type": enrichment.role_type,
                "seniority_level": enrichment.seniority_level,
                "ingested_at": utc_now(),
            }
        )

    df = pd.DataFrame(rows)
    return ensure_columns(df)


def normalize_csv(df: pd.DataFrame) -> pd.DataFrame:
    # Expect CSV has at least columns from sample; tolerate missing.
    df = df.copy()
    def getcol(name: str) -> pd.Series:
        return df[name] if name in df.columns else pd.Series([None] * len(df))

    titles = getcol("title").fillna("").map(normalize_whitespace)
    descs = getcol("description").fillna("").map(normalize_whitespace)
    urls = getcol("job_url").fillna("").map(normalize_whitespace)
    companies = getcol("company").fillna("").map(normalize_whitespace)
    pubs_raw = getcol("publication_date")

    pubs = pubs_raw.map(parse_datetime)
    job_hashes = []
    skills = []
    roles = []
    seniorities = []
    for title, desc, url, company, pub_dt in zip(titles, descs, urls, companies, pubs):
        base_for_hash = (url or "").strip() or f"{title}|{company}|{pub_dt.isoformat() if pub_dt else ''}"
        job_hashes.append(sha256_hex(base_for_hash))
        enr = enrich_job(title, desc)
        skills.append(enr.skills)
        roles.append(enr.role_type)
        seniorities.append(enr.seniority_level)

    out = pd.DataFrame(
        {
            "job_hash": job_hashes,
            "job_id": getcol("job_id").astype(str).replace({"nan": None}),
            "source": getcol("source").fillna("csv"),
            "title": titles,
            "company": companies,
            "location": getcol("location").fillna("").map(normalize_whitespace),
            "is_remote": getcol("is_remote").fillna(False).astype(bool),
            "category": getcol("category").fillna("").map(normalize_whitespace),
            "publication_date": pubs,
            "job_url": urls.replace({"": None}),
            "description": descs,
            "skills_extracted": skills,
            "role_type": roles,
            "seniority_level": seniorities,
            "ingested_at": utc_now(),
        }
    )
    return ensure_columns(out)


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in UNIFIED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[UNIFIED_COLUMNS]


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    # Prefer latest publication_date when duplicates
    df["_pub_sort"] = df["publication_date"].fillna(pd.Timestamp.min)
    df = df.sort_values(by=["job_hash", "_pub_sort"], ascending=[True, False])
    df = df.drop_duplicates(subset=["job_hash"], keep="first").drop(columns=["_pub_sort"])
    return df


def transform(api_jobs: list[dict[str, Any]], csv_df: pd.DataFrame) -> pd.DataFrame:
    api_df = normalize_adzuna(api_jobs)
    csv_norm = normalize_csv(csv_df)
    combined = pd.concat([api_df, csv_norm], ignore_index=True)
    combined = deduplicate(combined)
    return combined

