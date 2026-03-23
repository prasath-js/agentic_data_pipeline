import logging
import os
import sys
from datetime import datetime

# Ensure src is in the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src.bronze.sales_bronze import sales_bronze_pipeline
from src.silver.sales_silver import sales_silver_pipeline
from src.gold.sales_gold import sales_gold_pipeline
from src.quality.data_quality import run_data_quality_checks

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Main entry point for the sales_pipeline ETL process.
    Orchestrates the Bronze, Silver, Gold, and Data Quality layers.
    """
    pipeline_name = "sales_pipeline"
    start_time = datetime.now()
    logger.info(f"Starting {pipeline_name} at {start_time}")

    # Define staging and output directories
    # Use environment variables for paths to support different environments
    base_staging_dir = os.getenv("STAGING_DIR", "data/staging")
    bronze_output_dir = os.path.join(base_staging_dir, "bronze")
    silver_output_dir = os.path.join(base_staging_dir, "silver")
    gold_output_dir = os.getenv("GOLD_OUTPUT_DIR", "data/output") # Gold layer output directory

    # Ensure directories exist
    os.makedirs(bronze_output_dir, exist_ok=True)
    os.makedirs(silver_output_dir, exist_ok=True)
    os.makedirs(gold_output_dir, exist_ok=True)

    try:
        logger.info("--- Running Bronze Layer ---")
        sales_csv_path = os.getenv("SALES_CSV_PATH", "data/input/sales.csv")
        bronze_file_path = sales_bronze_pipeline(sales_csv_path, bronze_output_dir)
        logger.info(f"Bronze layer completed. Output: {bronze_file_path}")

        logger.info("--- Running Silver Layer ---")
        silver_file_path = sales_silver_pipeline(bronze_file_path, silver_output_dir)
        logger.info(f"Silver layer completed. Output: {silver_file_path}")

        logger.info("--- Running Gold Layer ---")
        # The gold layer directly outputs to the final target,
        # which is a local file specified by gold_output_dir.
        gold_output_path = sales_gold_pipeline(silver_file_path, gold_output_dir)
        logger.info(f"Gold layer completed. Output: {gold_output_path}")

        logger.info("--- Running Data Quality Checks ---")
        # Run quality checks on the Gold layer output
        quality_passed = run_data_quality_checks(gold_output_path)
        if quality_passed:
            logger.info("Data quality checks passed successfully for the Gold layer.")
        else:
            logger.error("Data quality checks failed for the Gold layer. Please investigate.")
            sys.exit(1) # Exit with an error code if quality checks fail

    except Exception as e:
        logger.exception(f"An error occurred during the {pipeline_name} pipeline execution.")
        sys.exit(1) # Exit with a non-zero code to indicate failure

    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Finished {pipeline_name} at {end_time}. Total duration: {duration}")

if __name__ == "__main__":
    main()
