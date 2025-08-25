FROM python:3.11-slim-bullseye

# Install system dependencies needed by dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Change WORKDIR to the dbt project folder
# This ensures dbt can auto-discover dbt_project.yml without needing --project-dir
WORKDIR /app/advantage_point

# Copy everything (project, profiles, configs, etc.)
# requirements.txt will still be copied from the repo root into /app/requirements.txt
COPY . .

# Install dbt dependencies (dbt-core, dbt-bigquery, etc.)
RUN pip install --no-cache-dir -r /app/requirements.txt

# Explicitly point dbt to the profiles directory *inside the project*
# This avoids having to pass --profiles-dir in Cloud Run job arguments
ENV DBT_PROFILES_DIR=/app/advantage_point/profiles

# Healthcheck for dbt
HEALTHCHECK CMD dbt --version || exit 1

# Default entrypoint: bash -c
# Cloud Run arguments will be passed as a single string after -c
# Example: args: ["dbt debug"] -> runs `bash -c "dbt debug"`
ENTRYPOINT ["bash", "-c"]