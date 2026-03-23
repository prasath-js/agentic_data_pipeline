# Sales Pipeline

This document provides an overview, setup instructions, and usage guide for the `sales_pipeline`.

## Table of Contents
1.  [Project Overview](#project-overview)
2.  [Architecture](#architecture)
3.  [Setup](#setup)
    *   [Prerequisites](#prerequisites)
    *   [Environment Variables](#environment-variables)
    *   [Installation](#installation)
4.  [Usage](#usage)
    *   [Running the Pipeline Manually](#running-the-pipeline-manually)
    *   [Scheduling](#scheduling)
5.  [Pipeline Details](#pipeline-details)
    *   [Sources](#sources)
    *   [Transformations](#transformations)
    *   [Output](#output)

## 1. Project Overview

The `sales_pipeline` is an ETL (Extract, Transform, Load) pipeline designed to process sales data daily. It follows a Medallion Lakehouse architecture (Bronze -> Silver -> Gold) to ensure data quality, governance, and optimized analytics.

The pipeline performs:
*   Ingestion of raw sales data from CSV files.
*   Cleaning, type casting, and PII masking of the ingested data.
*   Aggregation of sales data for analytical purposes.
*   Outputs the final aggregated data to a local file system.

## 2. Architecture

The pipeline adheres to the Medallion Architecture:

*   **Bronze Layer**: Ingests raw sales data from the source (CSV) and stores it as raw Parquet files in a staging area. No transformations are applied at this stage, ensuring data immutability.
*   **Silver Layer**: Reads the raw Parquet data from the Bronze layer, applies data cleaning, type casting, and PII masking (SHA-256 hashing for `customer_name`, `customer_email`, `product_name`). The cleaned data is then stored as Parquet files in another staging area.
*   **Gold Layer**: Reads the processed Parquet data from the Silver layer, performs aggregations (total sales and quantity by `order_date` and `region`), and writes the final analytical-ready data to the specified output destination (local file).

## 3. Setup

### Prerequisites

*   Python 3.8+
*   `pip` (Python package installer)

### Environment Variables

The pipeline relies on environment variables for configuration, especially for paths. Please set the following variables before running the pipeline:

*   `SALES_BRONZE_LANDING_PATH`: The local file path where raw (Bronze) Parquet files will be stored. E.g., `/app/data/bronze/sales`
*   `SALES_SILVER_LANDING_PATH`: The local file path where cleaned (Silver) Parquet files will be stored. E.g., `/app/data/silver/sales`
*   `SALES_GOLD_OUTPUT_PATH`: The local file path where final aggregated (Gold) data will be stored. E.g., `/app/data/gold/sales/aggregated_sales.csv`
*   `SALES_SOURCE_CSV_PATH`: The local file path to the source CSV file for sales data. E.g., `/app/data/source/sales.csv`

Example for setting environment variables (Linux/macOS):

```bash
export SALES_BRONZE_LANDING_PATH="/app/data/bronze/sales"
export SALES_SILVER_LANDING_PATH="/app/data/silver/sales"
export SALES_GOLD_OUTPUT_PATH="/app/data/gold/sales/aggregated_sales.csv"
export SALES_SOURCE_CSV_PATH="/app/data/source/sales.csv"
```

For Windows, use `set` instead of `export`.

### Installation

1.  **Clone the repository (if applicable) or navigate to the project directory.**

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    ```

3.  **Activate the virtual environment:**
    *   On Linux/macOS:
        ```bash
        source venv/bin/activate
        ```
    *   On Windows:
        ```bash
        .\venv\Scripts\activate
        ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## 4. Usage

### Running the Pipeline Manually

The pipeline is structured into three main stages: Bronze, Silver, and Gold. Each stage can be run independently or sequentially.

To run the entire pipeline:

1.  **Ensure all environment variables are set** as described in the [Environment Variables](#environment-variables) section.
2.  **Ensure your source CSV file exists** at the path specified by `SALES_SOURCE_CSV_PATH`.
3.  **Execute the main orchestration script** (assuming it's named `main.py` or similar, or run each stage individually):

    You would typically run each stage script sequentially:

    ```bash
    python src/bronze/sales_pipeline.py
    python src/silver/sales_pipeline.py
    python src/gold/sales_pipeline.py
    ```

    Or, if an orchestration script is provided:
    ```bash
    python src/main_orchestrator.py # (if such a file exists)
    ```

### Scheduling

This pipeline is designed for daily execution, typically managed by a cron job or similar scheduler.

Example cron entry for daily execution (e.g., at 2:00 AM):

```cron
0 2 * * * /path/to/your/project/venv/bin/python /path/to/your/project/src/bronze/sales_pipeline.py && \
/path/to/your/project/venv/bin/python /path/to/your/project/src/silver/sales_pipeline.py && \
/path/to/your/project/venv/bin/python /path/to/your/project/src/gold/sales_pipeline.py >> /var/log/sales_pipeline.log 2>&1
```

**Note**:
*   Replace `/path/to/your/project/venv/bin/python` with the actual path to your Python interpreter within the virtual environment.
*   Replace `/path/to/your/project/src/` with the actual path to your pipeline scripts.
*   Ensure the environment variables are sourced within the cron job's execution context, or hardcode them directly into the cron job script if necessary (not recommended for secrets). A common practice is to wrap the commands in a shell script that first sets the environment variables.

## 5. Pipeline Details

### Sources

*   **sales**:
    *   **Type**: CSV
    *   **Mode**: Full refresh (reads the entire source file each run)
    *   **Columns**: `order_id`, `customer_id`, `customer_name`, `customer_email`, `product_id`, `product_name`, `quantity`, `unit_price`, `total_amount`, `order_date`, `region`, `status`
    *   **PII Columns**: `customer_name`, `customer_email`, `product_name`

### Transformations

The pipeline performs the following key transformations:

*   **Bronze Layer**:
    *   Reads raw sales data from the CSV source.
    *   Writes data directly to Parquet files in the Bronze landing path without transformation.
*   **Silver Layer**:
    *   Reads raw sales data from the Bronze Parquet files.
    *   **Type Casting**: Converts `quantity`, `unit_price`, `total_amount` to appropriate numerical types and `order_date` to datetime objects.
    *   **PII Masking**: Applies SHA-256 hashing to `customer_name`, `customer_email`, and `product_name` for anonymization.
    *   Writes cleaned and masked data to Parquet files in the Silver landing path.
*   **Gold Layer**:
    *   Reads processed sales data from the Silver Parquet files.
    *   **Aggregation**: Groups data by `order_date` and `region`, then calculates the `total_sales` (sum of `total_amount`) and `total_quantity` (sum of `quantity`).

### Output

*   **Type**: Local File
*   **Authentication**: None
*   **Destination**: The aggregated data from the Gold layer will be written to the local file path specified by `SALES_GOLD_OUTPUT_PATH`. The output format will typically be CSV, but this can be configured within the Gold layer script.
