import os
import logging
import pandas as pd
from typing import Dict, Any

# Configure logger for the gold layer
logger = logging.getLogger(__name__)

def gold_local_files(silver_df: pd.DataFrame) -> None:
    """
    Aggregates silver layer data and writes the results to local CSV files.

    This function performs aggregations on the cleaned and transformed data
    from the silver layer, such as calculating total sales per region and status,
    and then stores the final aggregated data in a specified local directory.

    Args:
        silver_df (pd.DataFrame): The DataFrame containing data processed
                                   by the silver layer. This DataFrame
                                   is expected to have columns like
                                   'region', 'status', and 'amount'.

    Returns:
        None: The function writes aggregated data to local files and does not
              return any value.
    """
    logger.info("Starting Gold layer processing: Aggregating data and writing to local files.")

    if silver_df.empty:
        logger.warning("Silver DataFrame is empty. No data to aggregate or write in the Gold layer.")
        return

    # Retrieve output path from environment variables
    gold_output_folder = os.getenv("GOLD_LOCAL_FILES_OUTPUT_FOLDER")
    if not gold_output_folder:
        logger.error("Environment variable 'GOLD_LOCAL_FILES_OUTPUT_FOLDER' not set. Cannot write gold output.")
        raise ValueError("GOLD_LOCAL_FILES_OUTPUT_FOLDER environment variable is missing.")

    # Ensure the output directory exists
    os.makedirs(gold_output_folder, exist_ok=True)
    logger.info(f"Ensured gold output directory exists: {gold_output_folder}")

    try:
        # Aggregation 1: Total amount per region and status
        logger.info("Aggregating total amount per region and status...")
        region_status_summary: pd.DataFrame = silver_df.groupby(['region', 'status'])['amount'].sum().reset_index()
        region_status_summary.rename(columns={'amount': 'total_amount'}, inplace=True)
        region_status_output_path = os.path.join(gold_output_folder, "total_amount_by_region_status.csv")
        region_status_summary.to_csv(region_status_output_path, index=False)
        logger.info(f"Aggregated total amount per region and status written to: {region_status_output_path}")

        # Aggregation 2: Count of orders per customer (top 10 customers)
        logger.info("Aggregating order count per customer...")
        customer_order_counts: pd.DataFrame = silver_df.groupby('customer_id').size().reset_index(name='order_count')
        top_10_customers: pd.DataFrame = customer_order_counts.sort_values(by='order_count', ascending=False).head(10)
        top_10_customers_output_path = os.path.join(gold_output_folder, "top_10_customers_by_order_count.csv")
        top_10_customers.to_csv(top_10_customers_output_path, index=False)
        logger.info(f"Top 10 customers by order count written to: {top_10_customers_output_path}")

        logger.info("Gold layer processing completed successfully.")

    except KeyError as ke:
        logger.error(f"Missing expected column for aggregation in Silver DataFrame: {ke}")
        raise
    except Exception as e:
        logger.error(f"An error occurred during Gold layer processing or writing to local files: {e}")
        raise

if __name__ == "__main__":
    # This block is for testing purposes only when running this file directly.
    # In a real pipeline, gold_local_files would be called from main.py
    logging.basicConfig(level=logging.INFO)
    logger.info("Running gold_local_files.py as a standalone script for testing.")

    # Setup dummy environment variable for testing
    os.environ["GOLD_LOCAL_FILES_OUTPUT_FOLDER"] = "./temp_gold_output"

    # Create a dummy silver_df for testing
    data: Dict[str, Any] = {
        'order_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'customer_id': ['C101', 'C102', 'C101', 'C103', 'C102', 'C104', 'C101', 'C105', 'C103', 'C104'],
        'customer_name': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***',
                          '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'email': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***',
                  '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'amount': [100.50, 200.00, 150.75, 50.00, 300.25, 75.50, 220.00, 120.00, 90.00, 180.00],
        'status': ['completed', 'pending', 'completed', 'cancelled', 'completed',
                   'pending', 'completed', 'completed', 'pending', 'completed'],
        'region': ['North', 'South', 'North', 'East', 'West',
                   'South', 'North', 'East', 'West', 'South'],
        'order_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                       '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10']
    }
    dummy_silver_df = pd.DataFrame(data)

    try:
        gold_local_files(dummy_silver_df)
        logger.info("Standalone test completed. Check './temp_gold_output' folder for generated files.")
    except Exception as e:
        logger.error(f"Standalone test failed: {e}")
    finally:
        # Clean up dummy environment variable
        del os.environ["GOLD_LOCAL_FILES_OUTPUT_FOLDER"]
        # Basic cleanup of created test directory if it's empty or you want to remove it
        # Be careful with this in production-like scenarios
        # import shutil
        # if os.path.exists("./temp_gold_output"):
        #     shutil.rmtree("./temp_gold_output")