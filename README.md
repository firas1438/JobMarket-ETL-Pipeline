# Job Market Data Engineering Pipeline

End-to-end (batch + streaming) data engineering pipeline for tech job postings:

- **Batch ingestion**: Remotive Jobs API (JSON) + local CSV file.
- **Streaming ingestion**: Kafka topic `jobs_live` with a producer (polls Remotive) and consumer (inserts into Postgres).
- **Storage**: PostgreSQL tables (`jobs_staging`, `jobs_clean`, `jobs_stream`, `daily_metrics`).
- **Visualization**: Streamlit dashboard reading directly from Postgres.

## Architecture

![Architecture](https://i.imgur.com/XrsLXkX.png)

## Tech stack (free/open-source)
- Python, requests, pandas
- PostgreSQL
- Apache Kafka (+ Zookeeper)
- Streamlit
- Docker Compose
- pytest

## What this pipeline does

This project collects tech job postings and turns them into simple, queryable analytics.

- **Batch (ETL)**:
  - Extract jobs from **Remotive API** and from the local **CSV**.
  - Transform: normalize both sources into one schema, deduplicate jobs, extract skills, and classify role/seniority.
  - Load into Postgres:
    - `jobs_staging` (temporary batch load)
    - `jobs_clean` (final cleaned/deduped/enriched table used by the dashboard)
    - `daily_metrics` (aggregations per day for fast dashboard queries)

- **Streaming (Kafka)**:
  - A producer polls Remotive periodically and publishes only new jobs to Kafka topic `jobs_live`.
  - A consumer reads `jobs_live` and writes those events into Postgres table `jobs_stream`.

- **Dashboard (Streamlit)**:
  - Reads from Postgres and displays totals, trends over time, roles, companies, and skills.

## Setup & Run

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `docker compose up -d postgres zookeeper kafka` | Start Postgres, Zookeeper, Kafka |
| 2 | `docker compose run --rm -e FULL_REFRESH=1 batch` | Load jobs from API + CSV |
| 3 | `docker compose up -d app` | Start dashboard |
| 4 | Open http://localhost:8501 | View dashboard |
| - | `docker compose up producer` (terminal 1) | Optional: streaming producer |
| - | `docker compose up consumer` (terminal 2) | Optional: streaming consumer |
| - | `docker compose run --rm app pytest -q` | Run tests |
| - | `docker compose down` | Stop all containers |
| - | `docker compose down -v` | Stop all containers + delete data |

**Local (no Docker):** `python -m venv .venv` → activate → `pip install -r requirements.txt` → `pytest -q`

## Data sources
- Remotive Jobs API (public API)
- Local CSV file at `data/raw/jobs.csv`

## Project structure

- **`app/`**: data engineering pipeline (etl, dashboard, logging)
  - **`app/pipeline/`**: batch ETL (`extract.py`, `transform.py`, `load.py`, `run_batch.py`)
  - **`app/streaming/`**: Kafka producer/consumer (`producer.py`, `consumer.py`)
  - **`app/dashboard/`**: Streamlit UI (`dashboard.py`)
  - **`app/config/`**: settings + DB connection helpers
  - **`app/utils/`**: logging, hashing/date helpers, skills/role classification rules
- **`db/`**: SQL files
  - `schema.sql` creates tables/indexes
  - `queries.sql` contains example queries used for analysis
- **`data/`**: local data sources
  - `data/raw/jobs.csv` is the file-based ingestion source
- **`tests/`**: pytest unit tests (transformations, parsing, SQL strings)

## Dashboard

This is an example view of the Streamlit dashboard once the pipeline has loaded data and the app is running on `http://localhost:8501`.

![Streamlit dashboard](https://i.imgur.com/OzRcMsZ.png)



