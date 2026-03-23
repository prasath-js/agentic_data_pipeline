# local_csv_processing ETL Pipeline

This document describes the `local_csv_processing` ETL pipeline, which processes local CSV files, transforms them, and outputs processed data to local files.

## 1. Overview

The `local_csv_processing` pipeline is designed to ingest order data from local CSV files, apply a series of transformations (including PII masking, date format standardization, and null handling), and produce a refined dataset. The pipeline follows a Medallion architecture, separating data processing into Bronze (raw ingestion), Silver (cleaned and conformed), and Gold (aggregated and presentation-ready) layers.

**Sources:**
*   `input_csv_folder`: Local CSV files containing order information.

**Columns:**
*   `input_csv_folder`: `order_id`, `customer_id`, `customer_name`, `email`, `amount`, `status`, `region`, `order_date`

**PII Columns Masked (Silver Layer):**
*   `customer_name`
*   `email`

**Conflicts Resolved:**
*   Non-ISO date format detected (`DD/MM/YYYY` in `order_date` from `input_csv_folder`) will be standardized.
*   PII columns (`customer_name`, `email`) will be masked.

**Output:**
*   Processed data will be written to local files (Gold layer output).

**Technology Stack:**
*   Python 3.11+
*   Pandas for data manipulation and transformations.
*   SQLAlchemy for potential database connections (though primarily local files in this iteration).

## 2. Setup Instructions

Follow these steps to set up and run the pipeline.

### 2.1. Clone the Repository (if applicable)