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
    if settings.remotive_api_key:
        headers["Authorization"] = f"Bearer {settings.remotive_api_key}"

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


def read_local_csv(path: str) -> pd.DataFrame:
    logger.info("reading_csv", extra={"extra": {"path": path}})
    df = pd.read_csv(path)
    logger.info("read_csv", extra={"extra": {"rows": int(df.shape[0])}})
    return df

