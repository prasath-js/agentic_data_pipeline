"""
Settings module for the local_csv_processing_pipeline.
Defines environment variables and default configuration.
"""

from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings, using Pydantic BaseSettings to read from .env 
    or OS environment variables.
    """
    # Pipeline Identifiers
    PIPELINE_NAME: str = "local_csv_processing_pipeline"

    # File System Configuration
    INPUT_FOLDER_PATH: str = "./data/input"
    OUTPUT_FOLDER_PATH: str = "./data/output"

    # Database Configuration (for SQLAlchemy architecture requirements)
    DB_USER: str = "etl_user"
    DB_PASSWORD: str = "etl_password"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "etl_db"
    DATABASE_URL: str = "sqlite:///pipeline_metadata.db"

    # Transformation Rules
    PII_COLUMNS_TO_MASK: List[str] = ["customer_name", "email"]
    INPUT_DATE_FORMAT: str = "%d/%m/%Y"
    TARGET_DATE_FORMAT: str = "%Y-%m-%d"

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )


# Global settings instance to be imported across the pipeline
settings = Settings()