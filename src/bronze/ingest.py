import os
import logging
import pandas as pd
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv(file_path: str, source_name: str) -> pd.DataFrame:
    """
    Loads data from a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.
        source_name (str): The name of the source (for logging purposes).

    Returns:
        pd.DataFrame: The loaded data.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        pd.errors.EmptyDataError: If the specified file is empty.
        Exception: For other errors during CSV reading.
    """
    logger.info(f"Attempting to load data from CSV source: '{source_name}' at '{file_path}'")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(df)} rows from '{source_name}'.")
        return df
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at '{file_path}' for source '{source_name}'.")
        raise
    except pd.errors.EmptyDataError:
        logger.warning(f"Warning: CSV file '{file_path}' for source '{source_name}' is empty.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An unexpected error occurred while reading CSV for source '{source_name}' at '{file_path}': {e}")
        raise

def write_parquet(df: pd.DataFrame, output_path: str, source_name: str) -> None:
    """
    Writes a pandas DataFrame to a Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        output_path (str): The full path to the output Parquet file.
        source_name (str): The name of the source (for logging purposes).

    Raises:
        Exception: For errors during Parquet writing.
    """
    logger.info(f"Attempting to write {len(df)} rows to Parquet for source '{source_name}' at '{output_path}'")
    try:
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully wrote data for source '{source_name}' to '{output_path}'.")
    except Exception as e:
        logger.error(f"An error occurred while writing Parquet for source '{source_name}' to '{output_path}': {e}")
        raise

def main() -> None:
    """
    Main function for the Bronze layer ingestion.
    Reads raw data from various sources and writes it to a Parquet staging area.
    """
    logger.info("Starting Bronze layer ingestion for sales_pipeline.")

    # Configuration from environment variables
    BRONZE_STAGING_PATH = os.getenv('BRONZE_STAGING_PATH', './data/bronze')
    SALES_CSV_PATH = os.getenv('SALES_CSV_PATH', './data/raw/sales.csv')

    # Ensure the bronze staging directory exists
    os.makedirs(BRONZE_STAGING_PATH, exist_ok=True)
    logger.info(f"Bronze staging path set to: {BRONZE_STAGING_PATH}")

    # --- Ingest Sales Data (CSV) ---
    source_name_sales = "sales"
    sales_output_filename = os.path.join(BRONZE_STAGING_PATH, f"{source_name_sales}_raw.parquet")

    try:
        sales_df = load_csv(SALES_CSV_PATH, source_name_sales)
        if not sales_df.empty:
            write_parquet(sales_df, sales_output_filename, source_name_sales)
        else:
            logger.warning(f"No data to write for source '{source_name_sales}' as the DataFrame is empty.")
    except Exception as e:
        logger.error(f"Failed to ingest sales data: {e}")

    logger.info("Bronze layer ingestion completed for sales_pipeline.")

if __name__ == "__main__":
    main()
