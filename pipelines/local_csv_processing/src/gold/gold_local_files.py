import pandas as pd
import os
import logging
from typing import Optional

# Configure logging for this module
logger = logging.getLogger(__name__)

def gold_local_files(silver_df: pd.DataFrame) -> Optional[str]:
    """
    Aggregates the silver layer DataFrame and writes the results to local files (CSV).

    This function performs the following steps:
    1. Aggregates key metrics (total value, quantity, amount, number of opportunities,
       number of transactions) by 'account_id' and 'close_date'.
    2. Retrieves the output directory and filename from environment variables.
    3. Writes the aggregated data to a CSV file in the specified output path.

    Args:
        silver_df (pd.DataFrame): The DataFrame from the silver layer,
                                  containing cleaned and transformed data.

    Returns:
        Optional[str]: The path to the generated gold file if successful, None otherwise.
    """
    if silver_df.empty:
        logger.warning("Silver DataFrame is empty. No data to process for Gold layer.")
        return None

    logger.info("Starting Gold layer processing for local files.")

    try:
        # Define aggregation logic
        # Aggregate by account_id and close_date to get daily/account summaries
        gold_df = silver_df.groupby(['account_id', 'close_date']).agg(
            total_value=('value', 'sum'),
            total_quantity=('quantity', 'sum'),
            total_amount=('amount', 'sum'),
            num_opportunities=('opportunity_id', 'nunique'),
            num_transactions=('transaction_id', 'nunique')
        ).reset_index()

        logger.info(f"Aggregated data to {gold_df.shape[0]} rows and {gold_df.shape[1]} columns.")
        logger.debug(f"Gold DataFrame head:\n{gold_df.head()}")

        # Retrieve output path from environment variables
        output_dir = os.getenv("GOLD_LOCAL_FILES_OUTPUT_DIR")
        output_filename = os.getenv("GOLD_LOCAL_FILES_OUTPUT_FILENAME", "aggregated_sales.csv")

        if not output_dir:
            logger.error("Environment variable 'GOLD_LOCAL_FILES_OUTPUT_DIR' is not set.")
            return None

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        output_filepath = os.path.join(output_dir, output_filename)

        # Write the aggregated DataFrame to a CSV file
        gold_df.to_csv(output_filepath, index=False)
        logger.info(f"Successfully wrote gold data to: {output_filepath}")
        return output_filepath

    except KeyError as e:
        logger.error(f"Missing expected column for aggregation: {e}. Please check silver_df schema.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during gold layer processing: {e}", exc_info=True)
        return None