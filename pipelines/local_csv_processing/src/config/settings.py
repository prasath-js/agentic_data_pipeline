import os
from pathlib import Path
from typing import Dict, List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuration settings for the local_csv_processing pipeline.
    Loads environment variables from a .env file and provides sensible defaults.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- General Pipeline Settings ---
    APP_ENV: str = Field(
        default="development", description="The application environment (e.g., development, production)"
    )
    LOG_LEVEL: str = Field(default="INFO", description="Global logging level (e.g., DEBUG, INFO, WARNING, ERROR)")
    PIPELINE_NAME: str = Field(default="local_csv_processing", description="Name of the ETL pipeline")

    # --- Bronze Layer Settings ---
    BRONZE_INPUT_CSV_PATH: Path = Field(
        default=Path("./data/bronze/input_csv_folder"),
        description="Local path to the folder containing raw input CSV files for bronze ingestion.",
    )
    BRONZE_OUTPUT_PATH: Path = Field(
        default=Path("./data/bronze/output"),
        description="Local path to save bronze layer output (e.g., staging CSVs/Parquet).",
    )

    # --- Silver Layer Settings ---
    SILVER_OUTPUT_PATH: Path = Field(
        default=Path("./data/silver/output"),
        description="Local path to save silver layer output (e.g., cleaned Parquet files).",
    )
    PII_COLUMNS_TO_MASK: List[str] = Field(
        default=["customer_name", "email"],
        description="List of columns identified as PII that need masking in the Silver layer.",
    )
    INPUT_CSV_DATE_FORMAT: str = Field(
        default="%d/%m/%Y", description="Expected date format for 'order_date' column in input CSVs (e.g., DD/MM/YYYY)."
    )
    REQUIRED_COLUMNS: Dict[str, List[str]] = Field(
        default={
            "input_csv_folder": [
                "order_id",
                "customer_id",
                "customer_name",
                "email",
                "amount",
                "status",
                "region",
                "order_date",
            ]
        },
        description="A dictionary mapping source names to their expected columns.",
    )
    CRITICAL_NULL_CHECK_COLUMNS: List[str] = Field(
        default=["order_id", "customer_id", "amount"],
        description="Columns where null values are not allowed and rows should be filtered.",
    )

    # --- Gold Layer Settings ---
    GOLD_OUTPUT_PATH: Path = Field(
        default=Path("./data/gold/output"),
        description="Local path to save gold layer aggregated output (e.g., final report CSV/Parquet).",
    )
    GOLD_OUTPUT_FILENAME: str = Field(
        default="aggregated_sales_data.csv",
        description="Filename for the final aggregated gold layer output.",
    )


def get_settings() -> Settings:
    """
    Provides a singleton instance of the Settings for consistent configuration access.
    """
    return Settings()