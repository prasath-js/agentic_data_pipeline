import logging
import time
import pandas as pd
from typing import NoReturn

from src.config.logging_config import configure_logging
from src.bronze.ingest_local_csv_input import ingest_local_csv_input
from src.silver.transform_silver import transform_silver
from src.gold.gold_local_files import gold_local_files

# Configure logging at the module level
configure_logging()
logger = logging.getLogger(__name__)

def run() -> NoReturn:
    """
    Main entry point for the local_csv_processing ETL pipeline.

    Orchestrates the execution of Bronze, Silver, and Gold layers,
    handling logging, timing, and error management.
    """
    pipeline_name = "local_csv_processing"
    start_time = time.time()
    logger.info(f"🚀 Starting ETL pipeline: {pipeline_name}")

    try:
        # Bronze Layer: Ingest raw data
        logger.info("📦 Starting Bronze layer: Ingesting local_csv_input data.")
        bronze_df: pd.DataFrame = ingest_local_csv_input()
        logger.info(f"✅ Bronze layer completed. Ingested {len(bronze_df)} rows.")

        # Silver Layer: Transform and clean data
        logger.info("✨ Starting Silver layer: Transforming data.")
        silver_df: pd.DataFrame = transform_silver(bronze_df)
        logger.info(f"✅ Silver layer completed. Transformed {len(silver_df)} rows.")

        # Gold Layer: Aggregate and write data
        logger.info("💰 Starting Gold layer: Aggregating and writing data.")
        gold_local_files(silver_df)
        logger.info("✅ Gold layer completed. Data written to local files.")

    except Exception as e:
        logger.exception(f"❌ ETL pipeline '{pipeline_name}' failed with an error: {e}")
        # Depending on requirements, you might want to re-raise the exception
        # or perform specific cleanup here.
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"🏁 ETL pipeline '{pipeline_name}' finished in {duration:.2f} seconds.")


if __name__ == "__main__":
    run()