from pydantic_settings import BaseSettings, SettingsConfigDict
import logging
import os

class Settings(BaseSettings):
    """
    Pydantic settings class to manage environment variables for the ETL pipeline.
    Settings are loaded from a .env file and can be overridden by actual environment variables.
    """
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Application Settings ---
    APP_NAME: str = "local_csv_processing_etl"
    LOG_LEVEL: str = "INFO"  # Default log level, e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL

    # --- Bronze Layer Settings ---
    # Path to the local CSV input file for the bronze layer ingestion
    BRONZE_LOCAL_CSV_INPUT_PATH: str = os.path.join("data", "bronze", "local_csv_input.csv")

    # --- Silver Layer Settings ---
    # Path to the output file for the silver layer transformed data
    SILVER_OUTPUT_PATH: str = os.path.join("data", "silver", "processed_data.csv")

    # --- Gold Layer Settings ---
    # Path to the output file for the gold layer aggregated data
    GOLD_OUTPUT_PATH: str = os.path.join("data", "gold", "final_report.csv")

# Create a singleton instance of settings
settings = Settings()