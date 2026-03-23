# local_csv_processing_pipeline

This repository contains an ETL (Extract, Transform, Load) pipeline designed to process local CSV data, transform it, and output the results to local files. The pipeline follows a Medallion architecture (Bronze, Silver, Gold layers) to ensure data quality and integrity.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Setup Instructions](#setup-instructions)
- [Configuration](#configuration)
- [How to Run](#how-to-run)
- [Input and Output](#input-and-output)

## Overview

The `local_csv_processing_pipeline` is an automated data pipeline that:
1. **Extracts** raw order data from local CSV files (Bronze layer).
2. **Transforms** the data by cleaning, masking PII, and standardizing formats (Silver layer).
3. **Aggregates** the processed data and **loads** it into a final output format (Gold layer).

## Architecture

The pipeline adheres to the Medallion architecture:

-   **Bronze Layer:** Raw data ingestion from `local_csv_data` CSV files without any transformations.
-   **Silver Layer:** Data cleaning, standardization, PII masking, and filtering. This layer ensures data quality and prepares it for analytical use.
-   **Gold Layer:** Aggregated and refined data, ready for consumption.

## Features

-   **Data Ingestion:** Reads order data from local CSV files.
-   **PII Masking:** Automatically masks sensitive PII columns such as `customer_name` and `email` to `***MASKED***`.
-   **Date Format Standardization:** Resolves date format conflicts (e.g., `DD/MM/YYYY`) to a consistent ISO format.
-   **Null Handling:** Removes rows with null values in critical columns (if specified).
-   **Modularity:** Clear separation of concerns into Bronze, Silver, and Gold layers.
-   **Logging:** Comprehensive logging for monitoring and debugging.
-   **Environment Variables:** Secure handling of sensitive configurations via environment variables.

## Setup Instructions

1.  **Clone the repository:**