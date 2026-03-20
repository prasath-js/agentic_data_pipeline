import pandas as pd
import os
import logging
from typing import Dict, Any

from src.db_connection.builder import ConnectionBuilder
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def ingest_local_csv_input() -> pd.DataFrame:
    """
    Ingests data from a local CSV file, representing the bronze layer for local_csv_input.

    This function reads a CSV file from a path specified in environment variables.
    It does not perform any transformations, only raw data ingestion.

    Returns:
        pd.DataFrame: A DataFrame containing the raw data from the CSV file.
    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        Exception: For other errors during ingestion.
    """
    settings = Settings()
    csv_file_path = os.getenv("BRONZE_LOCAL_CSV_PATH")

    if not csv_file_path:
        logger.error("BRONZE_LOCAL_CSV_PATH environment variable not set.")
        raise ValueError("CSV file path not configured.")

    logger.info(f"Attempting to ingest data from local CSV: {csv_file_path}")

    try:
        # Use the ConnectionBuilder to get a local_files_connector
        connector_config: Dict[str, Any] = {
            "type": "local_files",
            "path": csv_file_path
        }
        connector = ConnectionBuilder.build_connector(connector_config)
        df = connector.read(file_path=csv_file_path, file_format="csv")

        logger.info(f"Successfully ingested {len(df)} rows from {csv_file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found at: {csv_file_path}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error ingesting data from {csv_file_path}: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    # Example usage for standalone testing
    # In a real scenario, BRONZE_LOCAL_CSV_PATH would be set in .env
    # For testing, create a dummy CSV
    import io
    test_csv_content = """opportunity_id,account_id,value,close_date,stage,transaction_id,customer_id,quantity,amount,transaction_date
1,101,1000.00,2023-01-15,Closed Won,T1,C1,10,100.00,2023-01-10
2,102,500.00,2023-02-20,Open,T2,C2,5,50.00,2023-02-15
3,101,200.00,2023-03-01,Closed Lost,T3,C1,2,20.00,2023-02-28
"""
    # Create a dummy file for demonstration
    dummy_csv_path = "temp_bronze_data.csv"
    with open(dummy_csv_path, "w") as f:
        f.write(test_csv_content)

    os.environ["BRONZE_LOCAL_CSV_PATH"] = dummy_csv_path
    os.environ["LOG_LEVEL"] = "INFO" # Ensure logging is configured for testing
    from src.config.logging_config import configure_logging
    configure_logging()

    try:
        bronze_df = ingest_local_csv_input()
        print("\nBronze DataFrame Head:")
        print(bronze_df.head())
        print(f"\nBronze DataFrame Shape: {bronze_df.shape}")
    except Exception as e:
        print(f"Failed to ingest data: {e}")
    finally:
        if os.path.exists(dummy_csv_path):
            os.remove(dummy_csv_path)
            print(f"Cleaned up {dummy_csv_path}")