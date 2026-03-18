from __future__ import annotations

import pandas as pd

from app.pipeline.transform import deduplicate, ensure_columns


def test_deduplicate_keeps_latest_by_publication_date():
    df = pd.DataFrame(
        [
            {"job_hash": "h1", "publication_date": "2026-03-01", "title": "Old"},
            {"job_hash": "h1", "publication_date": "2026-03-02", "title": "New"},
            {"job_hash": "h2", "publication_date": "2026-03-01", "title": "Only"},
        ]
    )
    df = ensure_columns(df)
    out = deduplicate(df)
    assert out.shape[0] == 2
    kept = out[out["job_hash"] == "h1"].iloc[0]
    assert kept["title"] == "New"

