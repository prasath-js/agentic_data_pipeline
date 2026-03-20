import pandas as pd
import os
import logging
from typing import Optional

# Configure logging for the module
# Assuming logging_config is set up in config/logging_config.py
# If not, a basic configuration for this file can be done:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# For this project, we'll assume the main application or a config module handles the full logging setup.
logger = logging.getLogger(__name__)


def ingest_local_csv_input() -> Optional[pd.DataFrame]:
    """
    Ingests raw data from a local CSV file specified by an environment variable.

    This function reads a CSV file into a pandas DataFrame without performing any
    transformations. The path to the CSV file is retrieved from the
    'LOCAL_CSV_INPUT_PATH' environment variable.

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame containing the raw data if
                                ingestion is successful, otherwise None.
    """
    csv_file_path = os.getenv("LOCAL_CSV_INPUT_PATH")

    if not csv_file_path:
        logger.error("Environment variable 'LOCAL_CSV_INPUT_PATH' not set. Cannot ingest CSV.")
        return None

    logger.info(f"Attempting to ingest data from local CSV: {csv_file_path}")

    try:
        df = pd.read_csv(csv_file_path)
        logger.info(f"Successfully ingested {len(df)} rows from {csv_file_path}.")
        logger.debug(f"First 5 rows of ingested data:\n{df.head().to_string()}")
        return df
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at '{csv_file_path}'. Please check the path.", exc_info=True)
        return None
    except pd.errors.EmptyDataError:
        logger.warning(f"Warning: CSV file '{csv_file_path}' is empty. Returning an empty DataFrame.")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file '{csv_file_path}': {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during CSV ingestion from '{csv_file_path}': {e}", exc_info=True)
        return None


if __name__ == "__main__":
    # Example usage when run directly
    # In a real scenario, this would be called by main.py
    # For testing, set a dummy environment variable
    os.environ["LOCAL_CSV_INPUT_PATH"] = "data/sample_opportunities.csv" # Adjust path as needed for local testing

    # Create a dummy CSV file for testing if it doesn't exist
    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(os.environ["LOCAL_CSV_INPUT_PATH"]):
        logger.info(f"Creating dummy CSV at {os.environ['LOCAL_CSV_INPUT_PATH']}")
        dummy_data = {
            "opportunity_id": [1, 2, 3],
            "account_id": [101, 102, 103],
            "value": [1000.0, 2500.0, 500.0],
            "close_date": ["2023-01-15", "2023/02/20", "2023-03-10"],
            "stage": ["Closed Won", "Open", "Closed Lost"],
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "customer_id": ["CUST001", "CUST002", "CUST003"],
            "quantity": [10, 20, 5],
            "amount": [100.0, 125.0, 100.0],
            "transaction_date": ["2023-01-01", "2023-02-05", "2023-03-01"]
        }
        pd.DataFrame(dummy_data).to_csv(os.environ["LOCAL_CSV_INPUT_PATH"], index=False)
        logger.info("Dummy CSV created.")

    # Basic logging setup for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO) # Set logger level for this module

    ingested_df = ingest_local_csv_input()

    if ingested_df is not None:
        logger.info("Ingestion successful. DataFrame head:")
        logger.info(f"\n{ingested_df.head().to_string()}")
    else:
        logger.error("Ingestion failed.")

    # Clean up dummy file and env var
    # os.remove(os.environ["LOCAL_CSV_INPUT_PATH"]) # Uncomment to clean up dummy file
    del os.environ["LOCAL_CSV_INPUT_PATH"]