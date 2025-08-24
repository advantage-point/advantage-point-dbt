# Use official Python slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy all files into container
COPY . .

# Set PYTHONPATH to allow absolute imports like `from utils...`
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Keep entry flexible to allow script args from Cloud Run Jobs
ENTRYPOINT ["python"]
