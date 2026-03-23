from __future__ import annotations

import pandas as pd

from app.pipeline.transform import UNIFIED_COLUMNS, transform


def test_transform_outputs_unified_schema():
    adzuna = [
        {
            "id": 123,
            "title": "Data Engineer",
            "company": {"display_name": "Acme"},
            "location": {"display_name": "Remote"},
            "category": {"label": "Software Development"},
            "created": "2026-03-10",
            "redirect_url": "https://example.com/jobs/123",
            "description": "Python SQL Docker Kafka",
        }
    ]
    csv_df = pd.DataFrame(
        [
            {
                "job_id": "csv-1",
                "source": "csv",
                "title": "Backend Engineer",
                "company": "WebCo",
                "location": "EU",
                "is_remote": False,
                "category": "Software Development",
                "publication_date": "2026-03-11",
                "job_url": "https://example.com/jobs/1",
                "description": "Python Postgres Docker",
            }
        ]
    )

    out = transform(adzuna, csv_df)
    assert list(out.columns) == UNIFIED_COLUMNS
    assert set(out["source"].unique()) == {"api", "csv"}

