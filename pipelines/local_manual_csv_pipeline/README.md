# local_manual_csv_pipeline

## Overview
`local_manual_csv_pipeline` is a robust ETL (Extract, Transform, Load) pipeline built using Python, Pandas, and SQLAlchemy. It follows the Medallion Architecture (Bronze, Silver, Gold) to process local file datasets systematically.

This pipeline ingests raw local CSV files from a designated input folder, cleans and standardizes the data (including PII masking and date formatting), and outputs the aggregated, clean data to local files.

## Architecture

* **Bronze Layer:** Ingests raw data from `input_folder` without applying transformations.
* **Silver Layer:** Applies business and data quality rules:
  * Masks Personally Identifiable Information (PII) for columns: `customer_name` and `email`.
  * Standardizes date formats, specifically resolving non-ISO date formats (`DD/MM/YYYY`) into ISO standard formats (`YYYY-MM-DD`).
  * Drops invalid rows and missing critical values.
* **Gold Layer:** Prepares the final, aggregated dataset for business consumption and writes the output back to a local directory.

## Data Schema

**Source: `input_folder`**
* `order_id`
* `customer_id`
* `customer_name` (PII - Masked in Silver)
* `email` (PII - Masked in Silver)
* `amount`
* `status`
* `region`
* `order_date`

## Setup Instructions

### Prerequisites
* Python 3.11 or higher
* pip (Python package installer)

### 1. Clone the repository and navigate to the project root