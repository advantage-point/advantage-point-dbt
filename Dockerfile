FROM python:3.11-slim-bullseye

# Install system dependencies needed by dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy everything (dbt project + profiles + configs)
COPY . .

# Install dbt dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Point dbt to bundled profiles directory
ENV DBT_PROFILES_DIR=/app/profiles

# Healthcheck for dbt
HEALTHCHECK CMD dbt --version || exit 1

# Default entrypoint
ENTRYPOINT ["bash", "-c"]
