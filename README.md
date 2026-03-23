# Job Market Data Engineering Pipeline

End-to-end (batch + streaming) data engineering pipeline for tech job postings:

- **Batch ingestion**: Adzuna Job Search API + local CSV file.
- **Streaming ingestion**: Kafka topic `jobs_live` with a producer (polls Adzuna) and consumer (inserts into Postgres).
- **Storage**: PostgreSQL tables (`jobs_staging`, `jobs_clean`, `jobs_stream`, `daily_metrics`).
- **Visualization**: Streamlit dashboard reading directly from Postgres.

## What this pipeline does

This project applies core data engineering principles through an end-to-end ETL architecture. It combines batch and streaming ingestion, enforces data quality through normalization and deduplication, and delivers reliable curated datasets for analytics. The pipeline is built for reproducibility (Docker), operational robustness (logging, retries, tests), and maintainability (modular components for ingestion, transformation, loading, and visualization).

## Architecture

![Architecture](https://i.imgur.com/WAdMAgY.png)

## General data lifecycle

1. Ingest jobs (API + CSV for batch, API for streaming).
2. Transform, normalize, clean, deduplicate (Spark), and enrich data.
3. Load into Postgres (`jobs_staging`, `jobs_clean`, `jobs_stream`).
4. Compute daily aggregates in `daily_metrics`.
5. Streamlit reads Postgres and shows dashboard metrics.
6. Repeat via `batch_scheduler` (scheduled batch) and producer/consumer (near-real-time stream).

## Data sources
- Adzuna Job Search API (public API)
- Local CSV file at `data/raw/jobs.csv`

## Tech stack (free/open-source)
- Python, requests, pandas
- Apache Spark (pyspark) for scalable batch transformations
- PostgreSQL
- Apache Kafka (+ Zookeeper)
- Streamlit
- Docker Compose
- pytest

## Setup & Run

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `docker compose up -d postgres zookeeper kafka` | Start Postgres, Zookeeper, Kafka |
| 2 | `docker compose up -d batch_scheduler` | Run scheduled batch ingestion |
| 3 | `docker compose up -d app` | Start dashboard |
| 4 | Open http://localhost:8501 | View dashboard |
| - | `docker compose up producer` (terminal 1) | Optional: streaming producer |
| - | `docker compose up consumer` (terminal 2) | Optional: streaming consumer |
| - | `docker compose run --rm -e FULL_REFRESH=1 batch` | Manual/on-demand batch run (testing, debugging, maintenance, full refresh) |
| - | `docker compose run --rm app pytest -q` | Run tests |
| - | `docker compose down` | Stop all containers |
| - | `docker compose down -v` | Stop all containers + delete data |

**Local (no Docker):** `python -m venv .venv` → activate → `pip install -r requirements.txt` → `pytest -q`

**Batch modes:** `batch_scheduler` is the default ongoing mode and repeats batch every `BATCH_INTERVAL_SECONDS` (default `1800`, i.e., 30 minutes). `batch` is one-shot and intended for manual/on-demand operations.

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