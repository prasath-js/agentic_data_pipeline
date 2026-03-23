import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

def gold_local_files(df: pd.DataFrame, output_path: str) -> None:
    """
    Aggregates Silver layer data and writes the Gold layer output to a local file.

    Args:
        df (pd.DataFrame): The transformed data from the Silver layer.
        output_path (str): The local file path where the Gold output will be saved.

    Returns:
        None
    """
    logger.info("Starting Gold layer aggregation for local_files output.")

    if df.empty:
        logger.warning("Input DataFrame is empty. Skipping Gold layer processing.")
        return

    try:
        expected_cols = {'region', 'status', 'amount', 'order_id'}
        
        if expected_cols.issubset(set(df.columns)):
            logger.info("Aggregating data by region and status.")
            
            # Ensure amount is numeric for accurate aggregation
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
            
            df_gold = df.groupby(['region', 'status']).agg(
                total_amount=('amount', 'sum'),
                order_count=('order_id', 'count')
            ).reset_index()
        else:
            logger.warning("Expected columns for aggregation missing. Proceeding with raw Silver data.")
            df_gold = df.copy()

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        logger.info("Writing Gold data to local file path: %s", output_path)
        df_gold.to_csv(output_path, index=False)

        logger.info("Gold layer processing completed successfully.")

    except Exception as e:
        logger.error("Error occurred during Gold layer processing: %s", str(e))
        raise