# Local CSV Processing ETL Pipeline

This ETL pipeline processes sales opportunity and transaction data from a local CSV file. It ingests raw data (Bronze), cleans and standardizes it by fixing date formats, removing invalid entries (Silver), and then aggregates the data before writing the final output to a new local CSV file (Gold). The pipeline follows a Medallion architecture for data quality and structure.

## Setup

1.  **Clone the repository:**