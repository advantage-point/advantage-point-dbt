FROM python:3.11-slim

# Install system dependencies required for dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
ENV PYTHONPATH="${PYTHONPATH}:/app"

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
