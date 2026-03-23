# Pipeline Flow: local_csv_processing_pipeline

## 1. Introduction

The `local_csv_processing_pipeline` is an ETL (Extract, Transform, Load) pipeline designed to process transactional data from a local CSV file. This pipeline adheres to a medallion architecture (Bronze, Silver, Gold layers) to ensure data quality, consistency, and proper handling of sensitive information, utilizing Pandas for data manipulation and SQLAlchemy for potential future database interactions (though current output is local files).

## 2. Pipeline Flow Overview

The pipeline executes in a sequential manner, moving data through three distinct layers:

1.  **Bronze Layer (Raw Data Ingestion):** Ingests raw data directly from the source (`local_csv_data`).
2.  **Silver Layer (Data Cleaning & Standardization):** Applies cleaning rules, resolves data conflicts, and masks PII.
3.  **Gold Layer (Data Aggregation & Output):** Aggregates processed data and writes it to the final output target.

## 3. Bronze Layer

*   **Purpose:** To ingest raw data from the specified source with minimal or no transformations. This layer serves as a historical archive of the original source data.
*   **Input:**
    *   `local_csv_data`: A local CSV file containing order details with columns: `order_id`, `customer_id`, `customer_name`, `email`, `amount`, `status`, `region`, `order_date`.
*   **Output:**
    *   A Pandas DataFrame representing the raw, untransformed data from `local_csv_data`.
*   **Business Rules Applied:**
    *   **Raw Ingestion:** Data is ingested directly from the local CSV file without any modifications.
    *   **Schema Preservation:** The original schema and data types from the source are preserved.
    *   No data cleaning, filtering, or transformation is performed at this stage.

## 4. Silver Layer

*   **Purpose:** To clean, standardize, and enhance the raw data from the Bronze layer. This layer ensures data quality and prepares the data for analysis by resolving conflicts and masking sensitive information.
*   **Input:**
    *   Pandas DataFrame(s) ingested from the Bronze layer (`local_csv_data`).
*   **Output:**
    *   A single, cleaned, standardized, and PII-masked Pandas DataFrame.
*   **Business Rules Applied:**
    *   **Date Format Resolution:** The `order_date` column from `local_csv_data`, detected as `DD/MM/YYYY` format, will be converted to a standardized ISO `YYYY-MM-DD` format.
    *   **PII Masking:** The following PII columns from `local_csv_data` will be masked with `***MASKED***`:
        *   `customer_name`
        *   `email`
    *   **Null Handling:** Critical columns will be evaluated for null values. Rows with nulls in `order_id` or `customer_id` will be removed to ensure data integrity.
    *   **Invalid Row Filtering:** Any rows that do not meet basic data validity checks (e.g., `amount` being non-negative) will be filtered out.
    *   **No Joins:** As there is only one source (`local_csv_data`), no joining operations are performed in this pipeline.

## 5. Gold Layer

*   **Purpose:** To aggregate and prepare the clean data from the Silver layer for final consumption, reporting, or further downstream processes. This layer serves as the source for business intelligence tools and applications.
*   **Input:**
    *   A cleaned and transformed Pandas DataFrame from the Silver layer.
*   **Output:**
    *   Local file(s) containing the aggregated data (e.g., a CSV file, Parquet file, or another suitable local format).
*   **Business Rules Applied:**
    *   **Aggregation:** Data from the Silver layer will be aggregated based on common reporting dimensions (e.g., total amount by region and status, daily order counts).
    *   **Output Target:** The final aggregated data will be written to local file(s) as per the specified output type.
    *   Specific aggregations (e.g., summing `amount` by `region` and `order_date`) will be applied to derive business insights.