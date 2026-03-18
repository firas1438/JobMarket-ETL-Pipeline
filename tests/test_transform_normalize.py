from __future__ import annotations

import pandas as pd

from app.pipeline.transform import UNIFIED_COLUMNS, transform


def test_transform_outputs_unified_schema():
    remotive = [
        {
            "id": 123,
            "title": "Data Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Worldwide",
            "category": "Software Development",
            "publication_date": "2026-03-10",
            "url": "https://remotive.com/jobs/123",
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

    out = transform(remotive, csv_df)
    assert list(out.columns) == UNIFIED_COLUMNS
    assert set(out["source"].unique()) == {"api", "csv"}

