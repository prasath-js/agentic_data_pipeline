from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings for the local_manual_csv_pipeline.
    Loads variables from the environment and the .env file.
    """
    
    # Pipeline configuration
    pipeline_name: str = Field(
        default="local_manual_csv_pipeline",
        description="Name of the ETL pipeline"
    )
    
    # Source paths (Local Files)
    input_folder_path: str = Field(
        default="./data/bronze_input",
        description="Local directory path for input source files"
    )
    
    # Output paths (Local Files)
    output_folder_path: str = Field(
        default="./data/gold_output",
        description="Local directory path for gold output files"
    )
    
    # Processing settings
    pii_columns: List[str] = Field(
        default=["customer_name", "email"],
        description="List of PII columns to mask in the Silver layer"
    )
    
    input_date_format: str = Field(
        default="%d/%m/%Y",
        description="Expected date format for input files (DD/MM/YYYY)"
    )

    # BaseSettings configuration for Pydantic v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


# Instantiate settings to be imported across the project
settings = Settings()