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

    def get(self, url, headers=None, timeout=None):
        return _FakeResponse(self.payload)

    def mount(self, *args, **kwargs):
        return None


def test_fetch_remotive_jobs_parses_jobs(monkeypatch):
    payload = {"jobs": [{"id": 1, "title": "Data Engineer"}]}

    monkeypatch.setattr(extract, "_requests_session_with_retry", lambda: _FakeSession(payload))
    jobs = extract.fetch_remotive_jobs(limit=10)

    assert isinstance(jobs, list)
    assert jobs[0]["id"] == 1

