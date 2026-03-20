# Pipeline Flow: `local_csv_processing`

This document outlines the data flow and transformations within the `local_csv_processing` ETL pipeline, which follows a Medallion Architecture (Bronze → Silver → Gold). The pipeline processes local CSV files, cleans and transforms the data, and then aggregates it for final output.

## 1. Bronze Layer

*   **Purpose**: Raw data ingestion from the source. The primary goal of the Bronze layer is to capture the data exactly as it appears in the source system, without any transformations or quality checks.
*   **Input**: `local_csv_input` (local CSV file).
    *   **Columns**: `opportunity_id`, `account_id`, `value`, `close_date`, `stage`, `transaction_id`, `customer_id`, `quantity`, `amount`, `transaction_date`
*   **Output**: A Pandas DataFrame containing the raw, untransformed data.
*   **Business Rules Applied**:
    *   None. This layer is purely for ingestion.

## 2. Silver Layer

*   **Purpose**: Data cleaning, standardization, and initial transformations. This layer focuses on improving data quality, resolving inconsistencies, and preparing the data for analytical use.
*   **Input**: Raw Pandas DataFrame from the Bronze layer.
*   **Output**: A cleaned, standardized Pandas DataFrame.
*   **Business Rules Applied**:
    *   **Date Format Conflict Resolution**:
        *   The `close_date` and `transaction_date` columns will be converted to a consistent ISO (YYYY-MM-DD) date format.
    *   **Invalid Row Filtering**:
        *   Rows where `amount` or `value` are less than or equal to 0 will be filtered out, as these are considered invalid transactions/opportunities.
    *   **Null Handling**:
        *   Rows with null values in critical columns such as `opportunity_id`, `account_id`, `amount`, and `transaction_date` will be removed or handled appropriately (e.g., imputation, depending on specific requirements; for this pipeline, rows with nulls in these critical identifiers/metrics will be dropped).
    *   **PII Masking**:
        *   No PII columns were identified for masking in this specific pipeline.
    *   **Joins**:
        *   No joins are performed in this pipeline as there is only one source (`local_csv_input`).

## 3. Gold Layer

*   **Purpose**: Data aggregation and preparation for final consumption. This layer provides highly refined, aggregated, and ready-to-use data sets, often optimized for specific reporting or analytical needs.
*   **Input**: Cleaned and standardized Pandas DataFrame from the Silver layer.
*   **Output**: Aggregated data written to a local file (e.g., CSV).
*   **Business Rules Applied**:
    *   **Aggregation**:
        *   The data will be aggregated to provide key metrics. This typically involves grouping by dimensions like `account_id` and `close_date`.
        *   Calculations may include:
            *   Total `amount` per `account_id` and `close_date`.
            *   Total `quantity` per `account_id` and `close_date`.
            *   Average `value` per `account_id`.
    *   **Output Storage**:
        *   The aggregated results will be stored in a local CSV file, making it accessible for downstream applications, reporting tools, or further analysis.