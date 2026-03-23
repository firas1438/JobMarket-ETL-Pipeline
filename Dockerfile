FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Spark requires Java at runtime.
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-21-jdk-headless \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY db /app/db
COPY data /app/data
COPY tests /app/tests
COPY README.md /app/README.md

# Default command is the dashboard; docker-compose will override for batch/streaming.
CMD ["streamlit", "run", "app/dashboard/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
