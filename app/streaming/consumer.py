from __future__ import annotations

import json

from kafka import KafkaConsumer
from sqlalchemy import text

from app.config.db import get_engine
from app.config.settings import settings
from app.utils.helpers import parse_datetime
from app.utils.logger import get_logger


logger = get_logger("streaming.consumer")


INSERT_STREAM = """
INSERT INTO jobs_stream (
  job_hash, job_id, source, title, company, location, is_remote, category,
  publication_date, job_url, description, ingested_at
)
VALUES (
  :job_hash, :job_id, :source, :title, :company, :location, :is_remote, :category,
  :publication_date, :job_url, :description, NOW()
)
ON CONFLICT (job_hash) DO UPDATE SET
  job_id = EXCLUDED.job_id,
  title = EXCLUDED.title,
  company = EXCLUDED.company,
  location = EXCLUDED.location,
  is_remote = EXCLUDED.is_remote,
  category = EXCLUDED.category,
  publication_date = EXCLUDED.publication_date,
  job_url = COALESCE(EXCLUDED.job_url, jobs_stream.job_url),
  description = EXCLUDED.description,
  ingested_at = EXCLUDED.ingested_at;
"""


def main() -> int:
    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id="jobs_stream_consumer",
        consumer_timeout_ms=1000,
    )

    engine = get_engine()
    logger.info(
        "consumer_started",
        extra={"extra": {"topic": settings.kafka_topic, "bootstrap": settings.kafka_bootstrap_servers}},
    )

    try:
        while True:
            processed = 0
            with engine.begin() as conn:
                for msg in consumer:
                    payload = msg.value
                    pub_dt = parse_datetime(payload.get("publication_date"))
                    params = {
                        "job_hash": payload.get("job_hash"),
                        "job_id": payload.get("job_id"),
                        "source": payload.get("source") or "api",
                        "title": payload.get("title"),
                        "company": payload.get("company"),
                        "location": payload.get("location"),
                        "is_remote": bool(payload.get("is_remote", True)),
                        "category": payload.get("category"),
                        "publication_date": pub_dt,
                        "job_url": payload.get("job_url"),
                        "description": payload.get("description"),
                    }
                    conn.execute(text(INSERT_STREAM), params)
                    processed += 1

            if processed:
                logger.info("batch_consumed", extra={"extra": {"messages": processed}})
    except KeyboardInterrupt:
        logger.info("consumer_stopped")
        return 0
    except Exception:
        logger.exception("consumer_failed")
        return 1
    finally:
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

