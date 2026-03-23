from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.config.settings import settings
from app.utils.logger import get_logger


logger = get_logger("pipeline.extract")


def _requests_session_with_retry() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def fetch_remotive_jobs(limit: int | None = None) -> list[dict[str, Any]]:
    """
    Fetch jobs from Remotive API.
    Returns a list of raw job dicts (as provided by the API).
    """
    url = settings.remotive_api_url
    headers: dict[str, str] = {}

    session = _requests_session_with_retry()
    logger.info("fetching_remotive_jobs", extra={"extra": {"url": url}})
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    if limit is not None:
        jobs = jobs[:limit]
    logger.info("fetched_remotive_jobs", extra={"extra": {"count": len(jobs)}})
    return jobs


def fetch_adzuna_jobs(
    max_jobs: int | None = None,
    *,
    start_page: int = 1,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch jobs from the Adzuna Job Search API.

    Adzuna returns results in pages; this function can:
    - fetch multiple pages starting at `start_page` (controlled by `max_pages`), and
    - optionally stop after collecting `max_jobs` results.
    """
    if settings.adzuna_app_id == "" or settings.adzuna_app_key == "":
        raise RuntimeError("Missing Adzuna credentials: set ADZUNA_APP_ID and ADZUNA_APP_KEY in .env")

    session = _requests_session_with_retry()

    results: list[dict[str, Any]] = []
    page = max(1, int(start_page))
    per_page = max(1, int(settings.adzuna_results_per_page))

    target: int | None
    if max_jobs is not None:
        target = None if max_jobs <= 0 else int(max_jobs)
    else:
        target = None if settings.adzuna_max_jobs <= 0 else int(settings.adzuna_max_jobs)

    page_end: int | None = None
    if max_pages is not None:
        page_end = page + max(1, int(max_pages)) - 1

    logger.info(
        "fetching_adzuna_jobs",
        extra={
            "extra": {
                "country": settings.adzuna_country,
                "page_size": per_page,
                "target_max_jobs": target,
                "what": settings.adzuna_what,
                "where": settings.adzuna_where,
                "start_page": start_page,
                "max_pages": max_pages,
            }
        },
    )

    while True:
        endpoint = f"{settings.adzuna_api_base_url}/{settings.adzuna_country}/search/{page}"
        params: dict[str, Any] = {
            "app_id": settings.adzuna_app_id,
            "app_key": settings.adzuna_app_key,
            "results_per_page": per_page,
            "what": settings.adzuna_what,
            "where": settings.adzuna_where,
            "content-type": "application/json",
        }

        resp = session.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        page_results = payload.get("results", []) if isinstance(payload, dict) else []
        if not page_results:
            break

        results.extend(page_results)
        logger.info(
            "adzuna_page_fetched",
            extra={"extra": {"page": page, "page_results": len(page_results), "total_collected": len(results)}},
        )

        if target is not None and len(results) >= target:
            break

        if page_end is not None and page >= page_end:
            break

        page += 1

    if target is not None and len(results) > target:
        results = results[:target]

    logger.info("fetched_adzuna_jobs", extra={"extra": {"count": len(results)}})
    return results


def read_local_csv(path: str) -> pd.DataFrame:
    logger.info("reading_csv", extra={"extra": {"path": path}})
    df = pd.read_csv(path)
    logger.info("read_csv", extra={"extra": {"rows": int(df.shape[0])}})
    return df

