import pandas as pd
import os
import logging
from typing import Optional

# Configure logging for this module
logger = logging.getLogger(__name__)

def ingest_local_csv_data(
    file_path_env_var: str = "LOCAL_CSV_DATA_PATH"
) -> Optional[pd.DataFrame]:
    """
    Ingests raw data from a local CSV file specified by an environment variable.

    This function reads a CSV file into a pandas DataFrame without performing
    any transformations, adhering to the Bronze layer rules. The path to the
    CSV file is retrieved from an environment variable to maintain security
    and configurability.

    Args:
        file_path_env_var (str): The name of the environment variable that
                                 stores the path to the local CSV file.
                                 Defaults to "LOCAL_CSV_DATA_PATH".

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame containing the raw data
                                from the CSV file, or None if the file cannot
                                be read or found.
    """
    csv_file_path = os.getenv(file_path_env_var)

    if not csv_file_path:
        logger.error(
            f"Environment variable '{file_path_env_var}' not set. "
            "Cannot ingest local CSV data without a file path."
        )
        return None

    logger.info(f"Attempting to ingest data from local CSV: {csv_file_path}")

    try:
        # Assuming the CSV has a header and is comma-separated by default
        df = pd.read_csv(csv_file_path)
        logger.info(
            f"Successfully ingested {len(df)} rows from {csv_file_path}. "
            f"Columns: {df.columns.tolist()}"
        )
        return df
    except FileNotFoundError:
        logger.error(f"Error: Local CSV file not found at {csv_file_path}")
        return None
    except pd.errors.EmptyDataError:
        logger.warning(f"Warning: Local CSV file at {csv_file_path} is empty.")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(
            f"Error parsing local CSV file at {csv_file_path}: {e}. "
            "Please check the file format and delimiter."
        )
        return None
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while reading local CSV file "
            f"at {csv_file_path}: {e}"
        )
        return None

if __name__ == "__main__":
    # This block is for demonstration and testing purposes only.
    # In a real pipeline, main.py orchestrates the calls.

    # Setup basic console logging for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a dummy CSV file for testing
    dummy_csv_content = """order_id,customer_id,customer_name,email,amount,status,region,order_date
1,101,John Doe,john.doe@example.com,150.00,Completed,North,15/01/2023
2,102,Jane Smith,jane.smith@example.com,200.50,Pending,South,20/02/2023
3,103,Peter Jones,peter.jones@example.com,75.25,Cancelled,West,05/03/2023
"""
    test_file_path = "test_local_csv_data.csv"
    with open(test_file_path, "w") as f:
        f.write(dummy_csv_content)

    os.environ["LOCAL_CSV_DATA_PATH"] = test_file_path

    logger.info("--- Starting standalone local CSV ingestion test ---")
    df_bronze = ingest_local_csv_data()

    if df_bronze is not None:
        logger.info("\n--- Ingested DataFrame (first 5 rows) ---")
        logger.info(df_bronze.head().to_string())
        logger.info(f"\nDataFrame shape: {df_bronze.shape}")
    else:
        logger.error("Failed to ingest local CSV data.")

    # Clean up the dummy CSV file
    if os.path.exists(test_file_path):
        os.remove(test_file_path)
        logger.info(f"Cleaned up dummy file: {test_file_path}")

    logger.info("--- Local CSV ingestion test finished ---")