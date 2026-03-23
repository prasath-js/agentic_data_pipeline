FROM python:3.9-slim-buster

WORKDIR /app

# Install system dependencies for any potential database drivers or other tools
# For example, if you were connecting to PostgreSQL, you might need libpq-dev
# If not explicitly needed, this can be minimal.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Ensure all src subdirectories have __init__.py files,
# though this should ideally be handled during development.
RUN find src -type d -exec touch {}/__init__.py \;

# Set environment variables for the pipeline
ENV PYTHONUNBUFFERED 1
ENV SALES_PIPELINE_STAGING_DIR "/app/data/staging"
ENV SALES_PIPELINE_OUTPUT_DIR "/app/data/output"

# Create necessary directories
RUN mkdir -p ${SALES_PIPELINE_STAGING_DIR}/bronze/sales \
           ${SALES_PIPELINE_STAGING_DIR}/silver/sales \
           ${SALES_PIPELINE_OUTPUT_DIR}/gold

# Define the command to run the pipeline.
# This example assumes a single entry point for the entire pipeline.
# For a daily schedule, you might trigger the main pipeline script.
CMD ["python", "src/sales_pipeline.py"]
