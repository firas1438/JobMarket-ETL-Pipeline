from __future__ import annotations

import json
import os
import time
from typing import Any

from kafka import KafkaProducer

from app.config.settings import settings
from app.pipeline.extract import fetch_remotive_jobs
from app.utils.helpers import normalize_whitespace, parse_datetime, sha256_hex
from app.utils.logger import get_logger


logger = get_logger("streaming.producer")


def _load_state(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"seen_hashes": []}
    except Exception:
        logger.exception("state_load_failed", extra={"extra": {"path": path}})
        return {"seen_hashes": []}


def _save_state(path: str, state: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def _job_to_minimal_message(j: dict[str, Any]) -> dict[str, Any]:
    job_id = str(j.get("id") or "") or None
    title = normalize_whitespace(j.get("title"))
    company = normalize_whitespace(j.get("company_name") or j.get("company"))
    pub_dt = parse_datetime(j.get("publication_date") or j.get("date"))
    url = normalize_whitespace(j.get("url") or j.get("job_url"))
    desc = normalize_whitespace(j.get("description") or "")
    base_for_hash = url or f"{title}|{company}|{pub_dt.isoformat() if pub_dt else ''}"
    job_hash = sha256_hex(base_for_hash)

    return {
        "job_hash": job_hash,
        "job_id": job_id,
        "source": "api",
        "title": title,
        "company": company,
        "location": normalize_whitespace(j.get("candidate_required_location") or j.get("location")),
        "is_remote": True,
        "category": normalize_whitespace(j.get("category")),
        "publication_date": pub_dt.isoformat() if pub_dt else None,
        "job_url": url or None,
        "description": desc,
    }


def main() -> int:
    state_path = settings.producer_state_path
    state = _load_state(state_path)
    seen = set(state.get("seen_hashes", []))

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
        acks="all",
        retries=5,
    )

    logger.info(
        "producer_started",
        extra={"extra": {"topic": settings.kafka_topic, "poll_seconds": settings.poll_seconds}},
    )
    try:
        while True:
            jobs = fetch_remotive_jobs()
            new_count = 0
            for j in jobs:
                msg = _job_to_minimal_message(j)
                if msg["job_hash"] in seen:
                    continue
                producer.send(settings.kafka_topic, key=msg["job_hash"], value=msg)
                seen.add(msg["job_hash"])
                new_count += 1

            producer.flush(timeout=30)

            # Keep state bounded
            if new_count:
                state["seen_hashes"] = list(seen)[-5000:]
                _save_state(state_path, state)

            logger.info("poll_complete", extra={"extra": {"new_jobs_emitted": new_count}})
            time.sleep(max(5, int(settings.poll_seconds)))
    except KeyboardInterrupt:
        logger.info("producer_stopped")
        return 0
    except Exception:
        logger.exception("producer_failed")
        return 1
    finally:
        try:
            producer.close(timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

