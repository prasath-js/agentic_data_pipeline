import sys
import time
import logging
import pandas as pd
from dotenv import load_dotenv

from src.config.settings import settings
from src.config.logging_config import configure_logging
from src.bronze.ingest_input_folder import ingest_input_folder
from src.silver.transform_silver import transform_silver
from src.gold.gold_local_files import gold_local_files

def run() -> None:
    """
    Main entry point for the local_manual_csv_pipeline ETL process.
    Executes the Bronze, Silver, and Gold layers in sequence.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Initialize logging configuration
    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting local_manual_csv_pipeline ETL pipeline.")
    total_start_time = time.time()

    try:
        # Retrieve configuration from settings
        input_path = settings.INPUT_FOLDER_PATH
        output_path = settings.OUTPUT_FOLDER_PATH

        # ---------------------------------------------------------------------
        # Bronze Layer
        # ---------------------------------------------------------------------
        logger.info("Starting Bronze layer ingestion...")
        bronze_start = time.time()
        
        df_bronze = ingest_input_folder(file_path=input_path)
        
        bronze_duration = time.time() - bronze_start
        logger.info(
            "Bronze layer completed. Processed %d rows in %.2f seconds.",
            len(df_bronze),
            bronze_duration
        )

        # ---------------------------------------------------------------------
        # Silver Layer
        # ---------------------------------------------------------------------
        logger.info("Starting Silver layer transformation...")
        silver_start = time.time()
        
        pii_columns = ["customer_name", "email"]
        df_silver = transform_silver(df=df_bronze, pii_columns=pii_columns)
        
        if len(df_silver) == 0:
            raise ValueError("Gold row count is 0. Aborting to prevent empty output files.")
        
        silver_duration = time.time() - silver_start
        logger.info(
            "Silver layer completed. Resulting dataset has %d rows. Time taken: %.2f seconds.",
            len(df_silver),
            silver_duration
        )

        # ---------------------------------------------------------------------
        # Gold Layer
        # ---------------------------------------------------------------------
        logger.info("Starting Gold layer export...")
        gold_start = time.time()
        
        gold_local_files(df=df_silver, output_path=output_path)
        
        gold_duration = time.time() - gold_start
        logger.info(
            "Gold layer completed. Output successfully written. Time taken: %.2f seconds.",
            gold_duration
        )

        # ---------------------------------------------------------------------
        # Pipeline Completion
        # ---------------------------------------------------------------------
        total_duration = time.time() - total_start_time
        logger.info(
            "Pipeline local_manual_csv_pipeline finished successfully in %.2f seconds.",
            total_duration
        )

    except Exception as e:
        logger.error("Pipeline failed due to a critical error: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    run()