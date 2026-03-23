import logging
import os
import sys
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src.bronze.sales_bronze import sales_bronze_layer
from src.silver.sales_silver import sales_silver_layer
from src.gold.sales_gold import sales_gold_layer
from src.quality.sales_quality import sales_quality_checks
from src.utils.config_loader import load_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline(config: dict) -> None:
    """
    Orchestrates the sales data pipeline, executing Bronze, Silver, Gold,
    and Quality layers in sequence.

    Args:
        config (dict): A dictionary containing pipeline configuration.
    """
    pipeline_name: str = config['pipeline_name']
    logger.info(f"Starting {pipeline_name} pipeline run.")

    try:
        # Bronze Layer
        logger.info("Executing Bronze Layer for sales data...")
        bronze_output_path: str = sales_bronze_layer(config)
        logger.info(f"Bronze Layer completed. Output: {bronze_output_path}")

        # Silver Layer
        logger.info("Executing Silver Layer for sales data...")
        silver_output_path: str = sales_silver_layer(bronze_output_path, config)
        logger.info(f"Silver Layer completed. Output: {silver_output_path}")

        # Quality Checks (after Silver Layer)
        logger.info("Executing Quality Checks on Silver Layer data...")
        quality_status: bool = sales_quality_checks(silver_output_path, config)
        if quality_status:
            logger.info("Quality checks passed for Silver Layer data.")
        else:
            logger.error("Quality checks failed for Silver Layer data. Aborting Gold Layer.")
            return # Exit if quality checks fail

        # Gold Layer
        logger.info("Executing Gold Layer for sales data...")
        gold_output_path: str = sales_gold_layer(silver_output_path, config)
        logger.info(f"Gold Layer completed. Output: {gold_output_path}")

    except (FileNotFoundError, IOError, KeyError, ValueError) as e: # Catch more specific exceptions
        logger.error(f"A data processing error occurred during the {pipeline_name} pipeline run: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred during the {pipeline_name} pipeline run: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info(f"Finished {pipeline_name} pipeline run.")

def main() -> None:
    """
    Main entry point for the sales pipeline. Loads configuration and
    initiates the pipeline run.
    """
    config_path: str = os.getenv('PIPELINE_CONFIG_PATH', 'config.yaml')
    config: dict = load_config(config_path)

    if not config:
        logger.error(f"Failed to load configuration from {config_path}. Exiting.")
        sys.exit(1)

    logger.info(f"Configuration loaded successfully from {config_path}")

    # Add run_id to config for traceability
    config['run_id'] = datetime.now().strftime("%Y%m%d%H%M%S")

    run_pipeline(config)

if __name__ == "__main__":
    main()
