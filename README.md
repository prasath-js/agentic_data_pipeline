# Sales Data ETL Pipeline

## Overview

The `sales_pipeline` is an Extract, Transform, Load (ETL) pipeline designed to process sales data. It ingests raw sales information from a CSV file, cleans and transforms it, masks sensitive information, and generates aggregated reports, storing the final output in a local file system.

## Architecture

This pipeline follows the Medallion Architecture pattern, comprising three distinct layers:

1.  **Bronze Layer**: Ingests raw sales data from the source (CSV) and stores it as raw Parquet files in a staging area. No transformations are applied at this stage, ensuring data immutability.
2.  **Silver Layer**: Reads the raw Parquet data from the Bronze layer, applies data cleaning, type casting, PII masking, and deduplication. The processed data is then stored as cleaned Parquet files.
3.  **Gold Layer**: Reads the cleaned Parquet data from the Silver layer, performs aggregations (e.g., total sales, quantity by date/region), and stores the final aggregated data in a specified output destination (local CSV).

## Pipeline Details

*   **Pipeline Name**: `sales_pipeline`
*   **Schedule**: Daily
*   **Output Target**: Local CSV file
*   **Runtime**: Python
*   **Framework**: Pandas for data transformations
*   **Scheduler**: Designed to be run by a cron job or similar task scheduler

## Data Sources

The pipeline processes data from the following source:

*   **`sales`**: A CSV file containing sales transaction records.
    *   **Mode**: Full (entire dataset is processed in each run).
    *   **Columns**: `order_id`, `customer_id`, `customer_name`, `customer_email`, `product_id`, `product_name`, `quantity`, `unit_price`, `total_amount`, `order_date`, `region`, `status`.
    *   **PII Columns (masked)**: `customer_name`, `customer_email`, `product_name`.

## Transformations

The pipeline applies the following key transformations:

*   **Type Casting**: Numeric fields (`quantity`, `unit_price`, `total_amount`) and date fields (`order_date`) are cast to appropriate data types.
*   **PII Masking**: Sensitive Personally Identifiable Information (PII) columns (`customer_name`, `customer_email`, `product_name`) are masked using SHA-256 hashing.
*   **Deduplication**: Records are deduplicated based on `order_id` to ensure unique sales transactions.
*   **Aggregation**: Total sales and quantity are aggregated by `order_date` and `region` in the Gold layer.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd sales_pipeline
```

### 2. Create a Virtual Environment

It's recommended to use a virtual environment to manage dependencies.

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 4. Prepare Source Data

Ensure your `sales.csv` file is accessible. For local testing, you might place it in a `data/raw` directory or specify its path using environment variables.

Example `sales.csv` content:

```csv
order_id,customer_id,customer_name,customer_email,product_id,product_name,quantity,unit_price,total_amount,order_date,region,status
1,101,John Doe,john.doe@example.com,P1,Laptop,1,1200.00,1200.00,2023-01-01,East,Completed
2,102,Jane Smith,jane.smith@example.com,P2,Mouse,2,25.50,51.00,2023-01-01,West,Completed
3,101,John Doe,john.doe@example.com,P3,Keyboard,1,75.00,75.00,2023-01-02,East,Pending
4,103,Peter Jones,peter.jones@example.com,P1,Laptop,1,1200.00,1200.00,2023-01-02,North,Completed
5,102,Jane Smith,jane.smith@example.com,P4,Monitor,1,300.00,300.00,2023-01-03,West,Completed
```

## Configuration

The pipeline uses environment variables for configuration, particularly for defining input and output paths.

Set the following environment variables before running the pipeline:

*   `SALES_BRONZE_SOURCE_PATH`: Path to the raw `sales.csv` file.
    *   Example: `/path/to/your/data/raw/sales.csv`
*   `SALES_BRONZE_STAGING_PATH`: Directory where Bronze layer Parquet files will be stored.
    *   Example: `/path/to/your/data/bronze`
*   `SALES_SILVER_STAGING_PATH`: Directory where Silver layer Parquet files will be stored.
    *   Example: `/path/to/your/data/silver`
*   `SALES_GOLD_OUTPUT_PATH`: Path to the final aggregated output file (e.g., `total_sales_by_region.csv`).
    *   Example: `/path/to/your/data/gold/total_sales_by_region.csv`

**Example Environment Variable Setup (Linux/macOS):**

```bash
export SALES_BRONZE_SOURCE_PATH="/home/user/sales_data/raw/sales.csv"
export SALES_BRONZE_STAGING_PATH="/home/user/sales_data/bronze"
export SALES_SILVER_STAGING_PATH="/home/user/sales_data/silver"
export SALES_GOLD_OUTPUT_PATH="/home/user/sales_data/gold/total_sales_by_region.csv"
```

**Example Environment Variable Setup (Windows PowerShell):**

```powershell
$env:SALES_BRONZE_SOURCE_PATH="C:\users\user\sales_data\raw\sales.csv"
$env:SALES_BRONZE_STAGING_PATH="C:\users\user\sales_data\bronze"
$env:SALES_SILVER_STAGING_PATH="C:\users\user\sales_data\silver"
$env:SALES_GOLD_OUTPUT_PATH="C:\users\user\sales_data\gold\total_sales_by_region.csv"
```

## Usage

Each layer of the pipeline can be executed independently. It is recommended to run them in sequence: Bronze, then Silver, then Gold.

Navigate to the `src` directory before running the scripts:

```bash
cd src
```

### 1. Run Bronze Layer

The Bronze layer reads the raw `sales.csv` and writes it to a Parquet file.

```bash
python -m bronze.sales_pipeline_bronze
```

This will create a `sales.parquet` file in the directory specified by `SALES_BRONZE_STAGING_PATH`.

### 2. Run Silver Layer

The Silver layer reads the Bronze Parquet file, cleans the data, masks PII, deduplicates, and writes to a new Parquet file.

```bash
python -m silver.sales_pipeline_silver
```

This will create a `sales_cleaned.parquet` file in the directory specified by `SALES_SILVER_STAGING_PATH`.

### 3. Run Gold Layer

The Gold layer reads the Silver Parquet file, performs aggregations, and writes the final report to a local CSV file.

```bash
python -m gold.sales_pipeline_gold
```

This will create the aggregated report (e.g., `total_sales_by_region.csv`) at the path specified by `SALES_GOLD_OUTPUT_PATH`.

## Scheduling

To automate the daily execution of the pipeline, you can use a cron job (on Linux/macOS).

1.  Open your crontab:
    ```bash
    crontab -e
    ```

2.  Add the following lines to schedule the pipeline to run daily at a specific time (e.g., 2 AM). Ensure your environment variables are correctly set within the cron environment or sourced from a profile.

    ```cron
    # Add environment variables for cron if not set globally
    # SHELL=/bin/bash
    # PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
    #
    # M H DOM MON DOW command
    0 2 * * * cd /path/to/your/sales_pipeline/src && \
        source /path/to/your/sales_pipeline/venv/bin/activate && \
        export SALES_BRONZE_SOURCE_PATH="/home/user/sales_data/raw/sales.csv" && \
        export SALES_BRONZE_STAGING_PATH="/home/user/sales_data/bronze" && \
        export SALES_SILVER_STAGING_PATH="/home/user/sales_data/silver" && \
        export SALES_GOLD_OUTPUT_PATH="/home/user/sales_data/gold/total_sales_by_region.csv" && \
        python -m bronze.sales_pipeline_bronze && \
        python -m silver.sales_pipeline_silver && \
        python -m gold.sales_pipeline_gold >> /path/to/your/sales_pipeline/pipeline.log 2>&1
    ```

    **Note**: Replace `/path/to/your/sales_pipeline` and the example environment variable paths with your actual paths. It is crucial to source the virtual environment and set environment variables within the cron job itself or ensure they are available in the cron's execution context.

## Logging

The pipeline uses Python's standard `logging` module. Log messages for each stage (Bronze, Silver, Gold) will be output to the console (and redirected to `pipeline.log` if running via cron as shown above). This helps monitor the pipeline's execution and diagnose any issues.
