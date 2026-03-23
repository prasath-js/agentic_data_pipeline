# Pipeline Flow: local_csv_processing_pipeline

## Overview
The `local_csv_processing_pipeline` follows a Medallion Architecture (Bronze, Silver, Gold layers) to process and transform order data. The pipeline is built using Pandas for in-memory data transformations and file system operations to read and write local files.

## Technology Stack
- **Engine**: Pandas
- **Architecture**: Medallion (Bronze -> Silver -> Gold)
- **Sources**: Local Files (`input_folder`)
- **Output**: Local Files

---

## Architecture Layers

### 1. Bronze Layer (Raw Ingestion)
**Purpose**: To extract raw data from the source systems exactly as it exists, with no transformations or data modifications.

- **Input**: CSV files located in the local directory (`input_folder`).
- **Input Columns**: `order_id`, `customer_id`, `customer_name`, `email`, `amount`, `status`, `region`, `order_date`.
- **Business Rules**: 
  - Read files dynamically using environment variables for paths.
  - Return the raw data as a Pandas DataFrame.
- **Output**: Raw Pandas DataFrame containing the exact contents of the source files.

### 2. Silver Layer (Cleansing and Conformance)
**Purpose**: To clean, filter, standardize, and secure the data. This layer applies critical business rules and resolves source conflicts.

- **Input**: Raw Pandas DataFrame from the Bronze layer.
- **Business Rules Applied**:
  - **PII Masking**: Identifies PII columns (`customer_name`, `email`) and replaces all values with the string `***MASKED***` to ensure data privacy.
  - **Date Format Standardization**: Detects non-ISO date format (`DD/MM/YYYY`) in the `order_date` column and converts it to standard ISO-8601 format (`YYYY-MM-DD`).
  - **Data Quality**: Removes rows containing nulls in critical business columns (e.g., `order_id`, `customer_id`).
  - **Filtering**: Filters out completely invalid rows based on status or structural integrity.
- **Output**: Cleansed, standardized, and secure Pandas DataFrame ready for analytics.

### 3. Gold Layer (Aggregation and Output)
**Purpose**: To prepare the final business-level aggregations and write the data to the target destination for end-user consumption or reporting.

- **Input**: Cleansed Pandas DataFrame from the Silver layer.
- **Business Rules Applied**:
  - Apply business-specific aggregations (e.g., aggregating total `amount` by `region` or `status`).
  - Structure the final dataset for the target downstream application.
- **Output**: Final Pandas DataFrame written back to the local file system (e.g., `output_folder/gold_data.csv`).