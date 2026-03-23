import logging
import sys
import time

from config.logging_config import configure_logging
from bronze.ingest_input_folder import ingest_input_folder
from silver.transform_silver import transform_silver
from gold.gold_local_files import export_gold_data


def run() -> None:
    """
    Entry point for the local_csv_processing_pipeline.
    Executes the Medallion architecture layers (Bronze, Silver, Gold) in sequence.
    """
    # Initialize logging using the configuration module
    configure_logging()
    logger = logging.getLogger(__name__)

    pipeline_start_time = time.time()
    logger.info("Starting local_csv_processing_pipeline execution.")

    try:
        # ---------------------------------------------------------------------
        # Bronze Layer: Ingestion
        # ---------------------------------------------------------------------
        logger.info("Executing Bronze layer: Ingesting data from input_folder.")
        bronze_start = time.time()
        
        df_bronze_input = ingest_input_folder()
        
        # QA Validation: Row Count Check
        bronze_rows = len(df_bronze_input)
        logger.info("Bronze layer extracted %d rows.", bronze_rows)
        if bronze_rows == 0:
            raise ValueError("Row count validation failed: Bronze dataset is empty.")
            
        logger.info("Bronze layer completed in %.2f seconds.", time.time() - bronze_start)

        # ---------------------------------------------------------------------
        # Silver Layer: Transformation
        # ---------------------------------------------------------------------
        logger.info("Executing Silver layer: Transforming data, standardizing dates, and masking PII.")
        silver_start = time.time()
        
        # Fix: Pass DataFrame directly to prevent dict AttributeError
        df_silver = transform_silver(df_bronze_input)
        
        # QA Validation: Row Count Check
        silver_rows = len(df_silver)
        logger.info("Silver layer produced %d rows.", silver_rows)
        if silver_rows == 0:
            raise ValueError("Row count validation failed: Silver dataset is empty after transformation.")
            
        logger.info("Silver layer completed in %.2f seconds.", time.time() - silver_start)

        # ---------------------------------------------------------------------
        # Gold Layer: Aggregation and Export
        # ---------------------------------------------------------------------
        logger.info("Executing Gold layer: Generating final aggregations and writing outputs.")
        gold_start = time.time()
        
        export_gold_data(df_silver)
        
        logger.info("Gold layer completed in %.2f seconds.", time.time() - gold_start)

        # ---------------------------------------------------------------------
        # Pipeline Completion
        # ---------------------------------------------------------------------
        total_time = time.time() - pipeline_start_time
        logger.info("Pipeline execution completed successfully in %.2f seconds.", total_time)

    except Exception as e:
        logger.error("Pipeline execution failed due to an error: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    run()