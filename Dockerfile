FROM python:3.11-slim-bullseye

# Install system dependencies needed by dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory to the dbt project folder
WORKDIR /app/advantage_point

# Copy requirements.txt first for Docker layer caching
COPY requirements.txt /app/requirements.txt

# Install dbt dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the repo (project + profiles + configs)
COPY . .

# Point dbt to the profiles directory INSIDE the project
ENV DBT_PROFILES_DIR=/app/advantage_point/profiles

# Healthcheck for dbt
HEALTHCHECK CMD dbt --version || exit 1

# Default entrypoint
ENTRYPOINT ["bash", "-c"]
