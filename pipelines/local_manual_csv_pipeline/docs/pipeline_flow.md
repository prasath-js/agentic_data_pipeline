# Pipeline Flow: local_manual_csv_pipeline

## Overview
This document outlines the data flow and transformation steps for the `local_manual_csv_pipeline`. The pipeline utilizes a Medallion architecture (Bronze, Silver, Gold) implemented with Pandas and SQLAlchemy.

**Pipeline Details:**
* **Sources**: `input_folder`
* **Source Type**: `local_files`
* **Output Target**: `local_files`
* **Technology**: Pandas, SQLAlchemy

---

## 1. Bronze Layer (Raw Ingestion)
The Bronze layer is responsible for raw data extraction. No transformations or masking occur at this stage.

### Source: `input_folder`
* **Type**: Local Files (CSV)
* **Extracted Columns**:
  * `order_id`
  * `customer_id`
  * `customer_name`
  * `email`
  * `amount`
  * `status`
  * `region`
  * `order_date`

---

## 2. Silver Layer (Cleansing & Transformation)
The Silver layer processes the raw Bronze data, applying business rules, resolving conflicts, and securing sensitive information.

### Transformations Applied:
* **PII Masking**: 
  * Detected PII columns: `customer_name`, `email`.
  * Action: Values are replaced with the static string `***MASKED***` using the pipeline's PII masker utility.
* **Date Format Standardization**:
  * Detected Conflict: Non-ISO date format `DD/MM/YYYY` in `input_folder`.
  * Action: The `order_date` column is parsed and standardized to ISO 8601 format (`YYYY-MM-DD`).
* **Data Quality**:
  * Nulls in critical columns are filtered out.
  * Invalid rows are dropped based on standard quality checks.

---

## 3. Gold Layer (Aggregation & Export)
The Gold layer consumes the clean, masked Silver data, applies any necessary business aggregations, and writes the finalized dataset to the destination.

* **Target Output**: `local_files`
* **Process**: Aggregates Silver data as required by the downstream business use-case and writes the output files to the designated local file destination.