from __future__ import annotations

import json
import os
import time
from typing import Any

from kafka import KafkaProducer

from app.config.settings import settings
from app.pipeline.extract import fetch_adzuna_jobs
from app.utils.helpers import normalize_whitespace, parse_datetime, sha256_hex
from app.utils.logger import get_logger


logger = get_logger("streaming.producer")


def _load_state(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"seen_hashes": [], "page": 1}
    except Exception:
        logger.exception("state_load_failed", extra={"extra": {"path": path}})
        return {"seen_hashes": [], "page": 1}


def _save_state(path: str, state: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def _job_to_minimal_message(j: dict[str, Any]) -> dict[str, Any]:
    job_id = str(j.get("id") or "") or None
    title = normalize_whitespace(j.get("title"))

    company_obj = j.get("company") or {}
    company = normalize_whitespace(
        company_obj.get("display_name") if isinstance(company_obj, dict) else j.get("company")
    )

    location_obj = j.get("location") or {}
    location = normalize_whitespace(
        location_obj.get("display_name") if isinstance(location_obj, dict) else j.get("location")
    )

    pub_dt = parse_datetime(j.get("created") or j.get("publication_date") or j.get("date"))
    url = normalize_whitespace(j.get("redirect_url") or j.get("url") or j.get("job_url"))
    desc = normalize_whitespace(j.get("description") or "")

    category_obj = j.get("category") or {}
    category = normalize_whitespace(
        category_obj.get("label") if isinstance(category_obj, dict) else j.get("category")
    )

    is_remote = "remote" in (location or "").lower()

    base_for_hash = url or f"{title}|{company}|{pub_dt.isoformat() if pub_dt else ''}"
    job_hash = sha256_hex(base_for_hash)

    return {
        "job_hash": job_hash,
        "job_id": job_id,
        "source": "api",
        "title": title,
        "company": company,
        "location": location,
        "is_remote": bool(is_remote),
        "category": category,
        "publication_date": pub_dt.isoformat() if pub_dt else None,
        "job_url": url or None,
        "description": desc,
    }


def main() -> int:
    state_path = settings.producer_state_path
    state = _load_state(state_path)
    seen = set(state.get("seen_hashes", []))
    current_page = int(state.get("page", 1) or 1)

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
            jobs = fetch_adzuna_jobs(
                start_page=current_page,
                max_pages=settings.adzuna_pages_per_poll,
            )
            new_count = 0
            for j in jobs:
                msg = _job_to_minimal_message(j)
                if msg["job_hash"] in seen:
                    continue
                producer.send(settings.kafka_topic, key=msg["job_hash"], value=msg)
                seen.add(msg["job_hash"])
                new_count += 1

            producer.flush(timeout=30)

            # Update producer state so we don't keep fetching the same page forever.
            # If the API returns no results, reset to page 1.
            if not jobs:
                current_page = 1
            else:
                current_page = current_page + settings.adzuna_pages_per_poll

            state["page"] = current_page
            # Keep state bounded (avoid unbounded growth).
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

