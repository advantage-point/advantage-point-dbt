FROM python:3.11-slim-bullseye

# Install system dependencies needed by dbt-bigquery
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt first for Docker layer caching
COPY requirements.txt /app/requirements.txt

# Install dbt dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the repo into /app
COPY . /app

# Set working directory to the dbt project folder
WORKDIR /app/advantage_point

# Point dbt to the profiles directory inside the project
ENV DBT_PROFILES_DIR=/app/advantage_point/profiles

# Healthcheck for dbt
HEALTHCHECK CMD dbt --version || exit 1

# Default entrypoint: bash -c
# Cloud Run args will be passed as a single string after -c
ENTRYPOINT ["bash", "-c"]