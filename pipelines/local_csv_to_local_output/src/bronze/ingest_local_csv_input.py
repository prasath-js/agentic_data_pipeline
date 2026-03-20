import pandas as pd
import logging
import os
from typing import Dict, Any

from src.config.settings import Settings

logger = logging.getLogger(__name__)

def ingest_local_csv_input(source_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Ingests data from a local CSV file into a Pandas DataFrame.

    Args:
        source_config (Dict[str, Any]): A dictionary containing configuration
                                        for the local CSV source, including 'path'.

    Returns:
        pd.DataFrame: A DataFrame containing the raw data from the CSV file.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        pd.errors.EmptyDataError: If the CSV file is empty.
        Exception: For other potential errors during file reading.
    """
    file_path = source_config.get("path")
    if not file_path:
        logger.error("CSV file path not provided in source configuration.")
        raise ValueError("CSV file path is required for local CSV ingestion.")

    logger.info(f"Attempting to ingest data from local CSV: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully ingested {len(df)} rows from {file_path}.")
        return df
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.warning(f"Warning: CSV file at {file_path} is empty. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An unexpected error occurred during CSV ingestion from {file_path}: {e}")
        raise

if __name__ == '__main__':
    # This block is for demonstration/testing purposes when running this file directly.
    # In a real pipeline, this function is called from main.py.
    from src.config.logging_config import setup_logging
    setup_logging()
    settings = Settings()
    
    # Create a dummy CSV for testing
    dummy_csv_path = "data/bronze/dummy_input.csv"
    os.makedirs(os.path.dirname(dummy_csv_path), exist_ok=True)
    dummy_data = {
        "opportunity_id": [1, 2, 3],
        "account_id": ["ACC001", "ACC002", "ACC001"],
        "value": [1000, 2500, 1500],
        "close_date": ["2023-01-15", "2023-02-20", "2023-01-25"],
        "stage": ["Closed Won", "Open", "Closed Lost"],
        "transaction_id": ["T001", "T002", "T003"],
        "customer_id": ["CUST001", "CUST002", "CUST001"],
        "quantity": [10, 5, 8],
        "amount": [950.50, 2400.00, 1400.75],
        "transaction_date": ["2023-01-10", "2023-02-18", "2023-01-22"]
    }
    pd.DataFrame(dummy_data).to_csv(dummy_csv_path, index=False)
    logger.info(f"Created dummy CSV at {dummy_csv_path}")

    # Override settings for direct run
    settings.bronze_settings.local_csv_input_path = dummy_csv_path

    try:
        source_config = {"path": settings.bronze_settings.local_csv_input_path}
        df_bronze = ingest_local_csv_input(source_config)
        logger.info("Bronze DataFrame head:\n%s", df_bronze.head())
        os.remove(dummy_csv_path) # Clean up dummy file
        logger.info(f"Cleaned up dummy CSV at {dummy_csv_path}")
    except Exception as e:
        logger.error(f"Test ingestion failed: {e}")