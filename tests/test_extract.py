from __future__ import annotations

import types

import pytest

from app.pipeline import extract


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:  # noqa: D401
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload: dict):
        self.payload = payload

    def get(self, url, headers=None, params=None, timeout=None, **_kwargs):
        return _FakeResponse(self.payload)

    def mount(self, *args, **kwargs):
        return None


def test_fetch_adzuna_jobs_parses_jobs(monkeypatch):
    payload = {
        "results": [
            {
                "id": 1,
                "title": "Data Engineer",
                "company": {"display_name": "Acme"},
                "location": {"display_name": "Remote"},
                "created": "2026-03-10",
                "redirect_url": "https://example.com/jobs/1",
                "description": "Python SQL Docker",
            }
        ]
    }

    class _FakeSettings:
        adzuna_app_id = "test"
        adzuna_app_key = "test"
        adzuna_api_base_url = "https://api.adzuna.com/v1/api/jobs"
        adzuna_country = "us"
        adzuna_what = "software engineer"
        adzuna_where = "remote"
        adzuna_results_per_page = 100
        adzuna_batch_max_pages = 10
        adzuna_pages_per_poll = 1
        adzuna_max_jobs = 0

    monkeypatch.setattr(extract, "_requests_session_with_retry", lambda: _FakeSession(payload))
    monkeypatch.setattr(extract, "settings", _FakeSettings())
    jobs = extract.fetch_adzuna_jobs(max_jobs=10)

    assert isinstance(jobs, list)
    assert jobs[0]["id"] == 1

