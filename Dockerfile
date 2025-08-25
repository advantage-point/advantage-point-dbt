FROM python:3.11-slim-bullseye

# Install system dependencies needed by dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory to the dbt project folder
# Ensures dbt finds dbt_project.yml at /app/advantage_point/dbt_project.yml
WORKDIR /app/advantage_point

# Copy requirements.txt first so Docker can cache this layer
COPY requirements.txt /app/requirements.txt

# Install dbt dependencies (dbt-core, dbt-bigquery, etc.)
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the repo (dbt project + profiles + configs)
COPY . .

# Point dbt to the profiles directory inside the project
ENV DBT_PROFILES_DIR=/app/advantage_point/profiles

# Healthcheck for dbt
HEALTHCHECK CMD dbt --version || exit 1

# Default entrypoint: bash -c
# Cloud Run arguments will be passed as a single string after -c
# Example: args: ["dbt debug"] -> runs `bash -c "dbt debug"`
ENTRYPOINT ["bash", "-c"]