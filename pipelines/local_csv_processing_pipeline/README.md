# local_csv_processing_pipeline

## Overview
The `local_csv_processing_pipeline` is a robust ETL pipeline built using Python, Pandas, and SQLAlchemy following the Medallion Architecture (Bronze, Silver, Gold). It is designed to ingest local file data, perform data quality checks, mask Personally Identifiable Information (PII), standardize date formats, and generate clean aggregations for downstream analytics.

## Processing Layers (Medallion Architecture)
*   **Bronze Layer**: Ingests raw CSV data from the `input_folder` exactly as-is.
*   **Silver Layer**: Cleanses and transforms the data. 
    *   Resolves non-ISO date formats (`DD/MM/YYYY` converted to standard ISO 8601 format).
    *   Masks PII columns (`customer_name` and `email`) with `***MASKED***`.
    *   Filters out invalid records and nulls in critical columns.
*   **Gold Layer**: Aggregates the cleansed Silver data to compute business-level metrics and writes the final output to local files.

## Input Data
*   **Source**: `input_folder` (Local Files)
*   **Schema**: 
    *   `order_id`
    *   `customer_id`
    *   `customer_name` (*PII - targeted for masking*)
    *   `email` (*PII - targeted for masking*)
    *   `amount`
    *   `status`
    *   `region`
    *   `order_date` (*Format: DD/MM/YYYY*)

## Output Data
*   **Target**: Local directory structure.
*   **Description**: Cleaned, PII-masked, and aggregated datasets derived from the input orders, structured for business consumption.

## Setup Instructions

1.  **Ensure Python 3.11+ is installed** on your system.
2.  **Create a virtual environment** (recommended):