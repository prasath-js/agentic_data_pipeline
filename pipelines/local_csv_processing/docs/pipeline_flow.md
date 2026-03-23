# Pipeline Flow Documentation: local_csv_processing

## Pipeline: local_csv_processing

### Overview
The `local_csv_processing` pipeline is an ETL (Extract, Transform, Load) process designed to ingest raw data from local CSV files, apply necessary transformations and cleansing, mask sensitive information, resolve data conflicts, and prepare the data for final consumption. It adheres to a medallion architecture, comprising Bronze, Silver, and Gold layers, to ensure data quality, lineage, and structured processing.

### Bronze Layer

*   **Purpose**: The Bronze layer is responsible for the raw ingestion of data directly from the specified source. Its primary goal is to create an exact, untransformed, and immutable copy of the source data, serving as a reliable historical record and the foundation for all subsequent processing. No data transformations, quality checks, or schema enforcements are performed at this stage.
*   **Input**: Local CSV files located within the `input_csv_folder`.
    *   **Source Columns**: `order_id`, `customer_id`, `customer_name`, `email`, `amount`, `status`, `region`, `order_date`
*   **Output**: A Pandas DataFrame containing the raw, unprocessed data directly from the source CSV files. This DataFrame is then passed as input to the Silver layer.
*   **Business Rules Applied**:
    *   None. Data is ingested as-is, preserving the original structure and content of the source files.

### Silver Layer

*   **Purpose**: The Silver layer focuses on cleansing, standardizing, and enriching the raw data received from the Bronze layer. This layer applies initial data quality rules, resolves data conflicts, and masks sensitive information, transforming the raw data into a clean, consistent, and structured format suitable for further analysis and aggregation.
*   **Input**: Pandas DataFrame from the Bronze layer, containing the raw data from `input_csv_folder`.
*   **Output**: A cleaned, standardized, and transformed Pandas DataFrame, ready for aggregation in the Gold layer.
*   **Business Rules Applied**:
    *   **PII Masking**: The columns `customer_name` and `email` are identified as Personally Identifiable Information (PII). Values in these columns will be masked with the string `***MASKED***` to comply with data privacy requirements.
    *   **Date Format Standardization**: The `order_date` column, detected with a `DD/MM/YYYY` format from the `input_csv_folder` source, will be converted to a standard ISO `YYYY-MM-DD` format to ensure consistency across the dataset.
    *   **Null Value Handling**: Although no specific critical columns were identified for null removal, the Silver layer framework supports general null value handling. Should nulls be detected in critical columns (e.g., `order_id`), rows containing these nulls would typically be filtered or their values imputed.
    *   **Filtering Invalid Rows**: No specific filtering rules for invalid rows were provided for this pipeline. However, the Silver layer provides the capability to filter out rows based on predefined business logic (e.g., `amount` being negative or `status` being invalid).
    *   **Join Operations**: No join keys were provided for this pipeline; therefore, no cross-source joins are performed in the Silver layer for `local_csv_processing`.

### Gold Layer

*   **Purpose**: The Gold layer is dedicated to aggregating and presenting the cleaned data from the Silver layer into final, highly consumable datasets. These datasets are optimized for specific business needs, such as reporting, analytics, or direct consumption by downstream applications. The final processed data is written back to local files.
*   **Input**: Pandas DataFrame from the Silver layer, containing the cleaned, standardized, and PII-masked data.
*   **Output**: Local files containing the aggregated and finalized data. The exact format and structure of these files depend on the specific output requirements.
*   **Business Rules Applied**:
    *   **Aggregation**: No specific aggregation rules (e.g., sum of amounts by region, count of orders by status) were explicitly defined for this pipeline. The Gold layer will typically perform aggregations as required for specific business reports or analytical models. For this pipeline, it will prepare the Silver data for direct output to local files, potentially with basic summarization if applicable.
    *   **Output Target**: The final processed and aggregated data is written to local files, making it readily available for consumption by end-users or other systems.