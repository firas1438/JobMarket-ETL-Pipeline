from __future__ import annotations

import os
import time

from app.pipeline.run_batch import main as run_batch_once
from app.utils.logger import get_logger


logger = get_logger("pipeline.scheduler")


def _get_interval_seconds() -> int:
    raw = os.getenv("BATCH_INTERVAL_SECONDS", "1800")
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "invalid_batch_interval",
            extra={"extra": {"value": raw, "fallback_seconds": 1800}},
        )
        return 1800
    if value < 60:
        logger.warning(
            "batch_interval_too_low",
            extra={"extra": {"value": value, "fallback_seconds": 60}},
        )
        return 60
    return value


def main() -> int:
    interval_seconds = _get_interval_seconds()
    logger.info(
        "scheduler_started",
        extra={"extra": {"interval_seconds": interval_seconds}},
    )

    while True:
        started_at = time.time()
        exit_code = run_batch_once()
        elapsed = int(time.time() - started_at)
        logger.info(
            "scheduler_cycle_finished",
            extra={"extra": {"exit_code": exit_code, "elapsed_seconds": elapsed}},
        )
        time.sleep(interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())

