import logging
import time
import pandas as pd
from typing import NoReturn

from config.logging_config import configure_logging
from src.bronze.ingest_input_csv_folder import ingest_input_csv_folder
from src.silver.transform_silver import transform_silver
from src.gold.gold_local_files import gold_local_files

# Initialize logger (will be configured by configure_logging())
logger = logging.getLogger(__name__)

def run() -> NoReturn:
    """
    Main entry point for the local_csv_processing ETL pipeline.
    Orchestrates the Bronze, Silver, and Gold layer operations,
    including logging and error handling.
    """
    configure_logging()
    
    pipeline_name = "local_csv_processing"
    logger.info(f"Starting {pipeline_name} ETL pipeline.")
    start_time = time.monotonic()

    bronze_df: pd.DataFrame
    silver_df: pd.DataFrame

    try:
        # --- Bronze Layer ---
        logger.info("Starting Bronze layer ingestion for 'input_csv_folder'...")
        bronze_df = ingest_input_csv_folder()
        logger.info(f"Bronze layer ingestion completed. Rows ingested: {len(bronze_df)}")

        # --- Silver Layer ---
        logger.info("Starting Silver layer transformation...")
        silver_df = transform_silver(bronze_df)
        logger.info(f"Silver layer transformation completed. Rows transformed: {len(silver_df)}")

        # --- Gold Layer ---
        logger.info("Starting Gold layer processing and output...")
        gold_local_files(silver_df)
        logger.info("Gold layer processing and output completed.")

    except Exception as e:
        logger.exception(f"An error occurred during the {pipeline_name} pipeline execution: {e}")
        # Depending on requirements, you might want to re-raise,
        # send alerts, or perform other cleanup here.
    finally:
        end_time = time.monotonic()
        elapsed_time = end_time - start_time
        logger.info(f"{pipeline_name} ETL pipeline finished. Total time: {elapsed_time:.2f} seconds.")

if __name__ == '__main__':
    run()