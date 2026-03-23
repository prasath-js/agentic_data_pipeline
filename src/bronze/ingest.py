import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_csv_source(source_path: str) -> pd.DataFrame:
    """
    Reads data from a CSV file.

    Args:
        source_path (str): The file path to the CSV source.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the raw data.
    """
    try:
        df = pd.read_csv(source_path)
        logger.info(f"Successfully read CSV from {source_path}. Rows read: {len(df)}")
        return df
    except FileNotFoundError:
        logger.error(f"CSV source file not found at {source_path}.")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV from {source_path}: {e}")
        raise

def write_parquet_to_staging(df: pd.DataFrame, staging_path: str, file_name: str) -> None:
    """
    Writes a pandas DataFrame to a Parquet file in the staging area.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        staging_path (str): The base directory for the staging area.
        file_name (str): The name of the Parquet file (e.g., "sales.parquet").
    """
    full_path = os.path.join(staging_path, file_name)
    os.makedirs(staging_path, exist_ok=True)
    try:
        df.to_parquet(full_path, index=False)
        logger.info(f"Successfully wrote {len(df)} rows to Parquet at {full_path}")
    except Exception as e:
        logger.error(f"Error writing Parquet to {full_path}: {e}")
        raise

def main() -> None:
    """
    Main function for the Bronze layer ingestion.
    Reads raw data from various sources and writes it to a staging area
    as Parquet files.
    """
    logger.info("Starting Bronze layer ingestion for sales_pipeline.")

    # Configuration from environment variables or default values
    # Staging area for bronze layer
    BRONZE_STAGING_PATH = os.getenv("BRONZE_STAGING_PATH", "data/bronze")
    
    # Source paths
    SALES_SOURCE_PATH = os.getenv("SALES_SOURCE_PATH", "data/raw/sales.csv")

    source_configs: Dict[str, Dict[str, Any]] = {
        "sales": {
            "type": "csv",
            "path": SALES_SOURCE_PATH,
            "output_file": "sales.parquet"
        }
    }

    current_date = datetime.now().strftime("%Y%m%d")
    bronze_output_dir = os.path.join(BRONZE_STAGING_PATH, f"sales_pipeline/{current_date}")

    for source_name, config in source_configs.items():
        logger.info(f"Processing source: {source_name}")
        df: pd.DataFrame = pd.DataFrame()
        try:
            if config["type"] == "csv":
                df = read_csv_source(config["path"])
            else:
                logger.warning(f"Unsupported source type '{config['type']}' for {source_name}. Skipping.")
                continue

            if not df.empty:
                write_parquet_to_staging(df, bronze_output_dir, config["output_file"])
            else:
                logger.warning(f"No data ingested for source {source_name}. DataFrame is empty.")

        except Exception as e:
            logger.error(f"Failed to ingest data for source {source_name}: {e}")
            # Depending on policy, decide whether to re-raise or continue
            # For bronze, we often want to fail fast if a critical source fails
            raise

    logger.info("Bronze layer ingestion completed for sales_pipeline.")

if __name__ == "__main__":
    main()
