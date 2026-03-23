import logging
import time
import os
import pandas as pd
from typing import Dict, List

from src.config.logging_config import configure_logging
from src.bronze.ingest_local_csv_data import ingest_local_csv_data
from src.silver.transform_silver import transform_silver
from src.gold.gold_local_files import gold_local_files

# Configure logging for the main pipeline
configure_logging()
logger = logging.getLogger(__name__)

def run() -> None:
    """
    Main function to run the local_csv_processing_pipeline ETL process.
    Orchestrates the Bronze, Silver, and Gold layers.
    """
    logger.info("Starting local_csv_processing_pipeline ETL process...")
    start_time = time.time()

    # Define pipeline parameters
    bronze_local_csv_data_path: str = os.getenv("BRONZE_LOCAL_CSV_DATA_PATH", "./data/input/local_csv_data.csv")
    gold_local_output_path: str = os.getenv("GOLD_LOCAL_OUTPUT_PATH", "./data/output/processed_data.csv")

    source_columns: Dict[str, List[str]] = {
        "local_csv_data": ["order_id", "customer_id", "customer_name", "email", "amount", "status", "region", "order_date"]
    }
    pii_columns_to_mask: List[str] = ["customer_name", "email"]
    # Configuration parameters below were previously passed to transform_silver.
    # They have been commented out or removed from the call to fix the "mismatched function signatures" error,
    # as `transform_silver` in `src/silver/transform_silver.py` is inferred not to accept them directly.
    # The specific handling of these transformations (date formats, nulls, joins) should be managed
    # within the `transform_silver` function itself or via a more generic configuration mechanism
    # if `transform_silver` is designed as a higher-level orchestrator.
    # date_format_conflicts: Dict[str, str] = {"order_date": "DD/MM/YYYY"}
    # critical_null_columns: List[str] = ["order_id", "customer_id", "amount", "order_date"]
    # join_keys: List[str] = [] # No joins specified for this single-source pipeline

    bronze_df: pd.DataFrame = pd.DataFrame()
    silver_df: pd.DataFrame = pd.DataFrame()

    # Bronze Layer: Ingestion
    logger.info("--- Bronze Layer: Ingesting local_csv_data ---")
    bronze_start_time = time.time()
    try:
        bronze_df = ingest_local_csv_data(
            file_path=bronze_local_csv_data_path,
            column_names=source_columns["local_csv_data"]
        )
        logger.info(f"Bronze layer completed for local_csv_data. Rows ingested: {len(bronze_df)}")
    except FileNotFoundError:
        logger.error(f"Error in Bronze layer: Input file not found at {bronze_local_csv_data_path}. Please check BRONZE_LOCAL_CSV_DATA_PATH environment variable.")
        return
    except Exception as e:
        logger.error(f"An unexpected error occurred in Bronze layer for local_csv_data: {e}", exc_info=True)
        return
    bronze_end_time = time.time()
    logger.info(f"Bronze layer processing time: {bronze_end_time - bronze_start_time:.2f} seconds")

    # Proceed to Silver only if Bronze was successful and returned data
    if not bronze_df.empty:
        # Silver Layer: Transformation
        logger.info("--- Silver Layer: Applying transformations ---")
        silver_start_time = time.time()
        try:
            # FIX: Adjusted the call to `transform_silver` to resolve "mismatched function signatures" (Critical Bug #1).
            # The parameters `date_format_conflicts`, `critical_null_columns`, and `join_keys` were
            # removed from the call as `transform_silver` is inferred not to accept them directly.
            # `main.py` continues to pass `pii_columns` as it's a configuration point,
            # though the issue of PII columns being hardcoded in `transform_silver.py` (flexibility issue)
            # would require changes within `transform_silver.py` itself.
            silver_df = transform_silver(
                df=bronze_df,
                pii_columns=pii_columns_to_mask
            )
            logger.info(f"Silver layer completed. Rows after transformation: {len(silver_df)}")
        except Exception as e:
            logger.error(f"An error occurred in Silver layer transformation: {e}", exc_info=True)
            return
        silver_end_time = time.time()
        logger.info(f"Silver layer processing time: {silver_end_time - silver_start_time:.2f} seconds")

        # Proceed to Gold only if Silver was successful and returned data
        if not silver_df.empty:
            # Gold Layer: Aggregation and Loading
            logger.info("--- Gold Layer: Aggregating and writing to local files ---")
            gold_start_time = time.time()
            try:
                gold_local_files(
                    df=silver_df,
                    output_path=gold_local_output_path
                )
                logger.info(f"Gold layer completed. Data written to: {gold_local_output_path}")
            except Exception as e:
                logger.error(f"An error occurred in Gold layer writing to local files: {e}", exc_info=True)
                return
            gold_end_time = time.time()
            logger.info(f"Gold layer processing time: {gold_end_time - gold_start_time:.2f} seconds")
        else:
            logger.warning("Silver layer produced an empty DataFrame. Skipping Gold layer.")
    else:
        logger.warning("Bronze layer produced an empty DataFrame. Skipping Silver and Gold layers.")

    end_time = time.time()
    total_duration = end_time - start_time
    logger.info(f"local_csv_processing_pipeline ETL process finished. Total duration: {total_duration:.2f} seconds.")

if __name__ == '__main__':
    run()