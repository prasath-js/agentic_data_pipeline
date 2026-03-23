import os
import logging
import pandas as pd
from typing import Dict, Any

# Configure logging for the module
logger = logging.getLogger(__name__)

def write_gold_data(silver_df: pd.DataFrame) -> None:
    """
    Aggregates the Silver layer DataFrame and writes the gold data to local CSV files.

    This function performs aggregations such as calculating total sales and order counts
    per region and status from the provided silver_df. The aggregated data is then
    saved into a CSV file in a specified output directory.

    Args:
        silver_df (pd.DataFrame): The DataFrame processed by the Silver layer,
                                  containing cleaned and transformed data.
                                  Expected columns include 'region', 'status', 'amount'.

    Returns:
        None: The function writes data to local files and does not return any value.
    """
    if silver_df.empty:
        logger.warning("Silver DataFrame is empty. No Gold data to aggregate or write.")
        return

    logger.info("Starting Gold layer aggregation and writing to local files.")

    try:
        # Define output directory from environment variables
        gold_output_dir = os.getenv("GOLD_LOCAL_FILES_OUTPUT_DIR", "./data/gold")
        os.makedirs(gold_output_dir, exist_ok=True)
        gold_output_path = os.path.join(gold_output_dir, "aggregated_orders.csv")

        logger.info(f"Aggregating data to generate gold output. Output path: {gold_output_path}")

        # Ensure 'order_date' is datetime for potential time-based aggregations, though not explicitly used below
        if 'order_date' in silver_df.columns:
            silver_df['order_date'] = pd.to_datetime(silver_df['order_date'], errors='coerce')

        # Example Aggregation 1: Total amount and order count per region
        region_aggregation = silver_df.groupby('region').agg(
            total_amount=('amount', 'sum'),
            number_of_orders=('order_id', 'nunique')
        ).reset_index()
        region_aggregation.rename(columns={'region': 'Region'}, inplace=True)
        logger.info("Aggregated total amount and order count per region.")

        # Example Aggregation 2: Total amount and order count per status
        status_aggregation = silver_df.groupby('status').agg(
            total_amount=('amount', 'sum'),
            number_of_orders=('order_id', 'nunique')
        ).reset_index()
        status_aggregation.rename(columns={'status': 'OrderStatus'}, inplace=True)
        logger.info("Aggregated total amount and order count per status.")

        # Example Aggregation 3: Total amount and order count per region and status
        region_status_aggregation = silver_df.groupby(['region', 'status']).agg(
            total_amount=('amount', 'sum'),
            number_of_orders=('order_id', 'nunique')
        ).reset_index()
        region_status_aggregation.rename(columns={'region': 'Region', 'status': 'OrderStatus'}, inplace=True)
        logger.info("Aggregated total amount and order count per region and status.")

        # For this specific pipeline, let's write the region_status_aggregation as the primary gold output
        final_gold_df = region_status_aggregation

        # Write the aggregated DataFrame to a local CSV file
        final_gold_df.to_csv(gold_output_path, index=False)
        logger.info(f"Successfully wrote aggregated gold data to {gold_output_path}")

    except KeyError as e:
        logger.error(f"Missing expected column for aggregation: {e}. Please check Silver layer output.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during Gold layer processing: {e}")
        raise

if __name__ == '__main__':
    # This block is for demonstrating the gold layer in isolation if needed for debugging.
    # In a real pipeline, `write_gold_data` would be called from main.py.
    # Set up a dummy DataFrame for testing
    logging.basicConfig(level=logging.INFO) # Basic logging setup for standalone run
    logger.info("Running gold_local_files.py in standalone test mode.")

    # Mock Silver DataFrame
    mock_silver_data = {
        'order_id': ['O1', 'O2', 'O3', 'O4', 'O5', 'O6'],
        'customer_id': ['C1', 'C2', 'C1', 'C3', 'C2', 'C4'],
        'customer_name': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'email': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'amount': [100.0, 250.5, 120.0, 300.0, 50.0, 180.0],
        'status': ['completed', 'pending', 'completed', 'cancelled', 'completed', 'pending'],
        'region': ['north', 'south', 'north', 'east', 'west', 'north'],
        'order_date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03', '2023-01-04', '2023-01-02']
    }
    mock_silver_df = pd.DataFrame(mock_silver_data)

    # Set a dummy output directory for testing
    os.environ["GOLD_LOCAL_FILES_OUTPUT_DIR"] = "./data/gold_test_output"

    try:
        write_gold_data(mock_silver_df)
        logger.info("Standalone Gold layer test completed successfully.")
    except Exception as e:
        logger.error(f"Standalone Gold layer test failed: {e}")