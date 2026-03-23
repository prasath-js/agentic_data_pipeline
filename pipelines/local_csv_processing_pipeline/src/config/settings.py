from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Union

class Settings(BaseSettings):
    """
    Pydantic settings for the local_csv_processing_pipeline.

    Loads environment variables from a .env file and provides default values.
    """
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    # --- General Pipeline Settings ---
    PIPELINE_NAME: str = "local_csv_processing_pipeline"
    LOG_LEVEL: str = "INFO"  # Default logging level

    # --- Bronze Layer Settings ---
    # Path to the raw local CSV data file
    BRONZE_LOCAL_CSV_DATA_PATH: str = "./data/raw/local_csv_data.csv"

    # --- Silver Layer Settings ---
    # Path where the silver layer will output its processed data (e.g., an intermediate CSV)
    SILVER_OUTPUT_PATH: str = "./data/processed/silver_data.csv"
    # List of PII columns to be masked in the Silver layer
    PII_COLUMNS: List[str] = ["customer_name", "email"]
    # Expected date format for the 'local_csv_data' source
    DATE_FORMAT_LOCAL_CSV_DATA: str = "DD/MM/YYYY"

    # --- Gold Layer Settings ---
    # Path where the final aggregated gold layer data will be written
    GOLD_OUTPUT_PATH: str = "./data/output/gold_aggregated_data.csv"
    # Column to use for aggregation in the Gold layer
    GOLD_AGGREGATION_COLUMN: str = "amount"
    # Column to group by for aggregation (e.g., 'region')
    GOLD_AGGREGATION_GROUP_BY_COLUMN: str = "region"

    # --- Database Connection Settings (Placeholders for SQLAlchemy if needed in future, not directly used for local files) ---
    # Although this pipeline processes local files, SQLAlchemy is specified as a technology.
    # These can be left with sensible defaults or empty if not used, or configured if a DB is involved.
    DB_CONNECTION_STRING: str = "sqlite:///:memory:" # Default to in-memory SQLite for minimal setup

# Instantiate settings for easy import
settings = Settings()