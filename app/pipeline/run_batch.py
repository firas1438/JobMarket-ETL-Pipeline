from __future__ import annotations

import os
import sys

from app.pipeline.extract import fetch_adzuna_jobs, read_local_csv
from app.pipeline.load import run_load
from app.pipeline.transform import transform
from app.config.settings import settings
from app.utils.logger import get_logger


logger = get_logger("pipeline.run_batch")


def main() -> int:
    csv_path = os.getenv("CSV_SOURCE_PATH", "data/raw/jobs.csv")
    try:
        api_jobs = fetch_adzuna_jobs(max_pages=settings.adzuna_batch_max_pages)
        csv_df = read_local_csv(csv_path)
        final_df = transform(api_jobs, csv_df)
        logger.info("batch_transformed", extra={"extra": {"rows": int(final_df.shape[0])}})
        run_load(final_df)
        return 0
    except Exception:
        logger.exception("batch_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

