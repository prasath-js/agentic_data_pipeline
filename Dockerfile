FROM python:3.10-slim-buster

# Set working directory
WORKDIR /app

# Install system dependencies required for psycopg2 or other database drivers
# In this case, we don't have a direct DB connection that requires system libs
# If we were connecting to Postgres using psycopg2, we'd need libpq-dev
# If we were connecting to MySQL using mysqlclient, we'd need default-libmysqlclient-dev
# For now, we assume standard dependencies are sufficient for local file I/O

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Set environment variables for the pipeline
ENV PIPELINE_NAME=sales_pipeline
ENV PYTHONUNBUFFERED=1

# Create necessary directories for logs and data
RUN mkdir -p /app/logs /app/data/bronze /app/data/silver /app/data/gold /app/config

# Define the command to run the pipeline
# This can be overridden when running the container
# For a daily schedule, a cron job on the host or an orchestrator would trigger this command.
# Here, we provide a placeholder command that runs the gold layer, assuming it orchestrates the full pipeline.
CMD ["python", "src/sales_pipeline/gold.py"]
