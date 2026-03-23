import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def create_regional_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate the silver DataFrame to create a regional summary.
    Groups by region and status, calculating total amount and order count.

    Args:
        df (pd.DataFrame): The cleaned and masked Silver layer DataFrame.

    Returns:
        pd.DataFrame: Aggregated Gold layer DataFrame.
    """
    logger.info("Aggregating silver data to generate regional sales summary.")
    try:
        if df.empty:
            logger.warning("Empty DataFrame received for aggregation.")
            return pd.DataFrame()

        # Convert amount to numeric to avoid aggregation errors
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

        # Aggregate: Total amount and count of orders per region and status
        agg_df = df.groupby(['region', 'status'], dropna=False).agg(
            total_amount=('amount', 'sum'),
            order_count=('order_id', 'count')
        ).reset_index()

        logger.info("Aggregation complete. Generated %d summary rows.", len(agg_df))
        return agg_df
    except Exception as e:
        logger.error("Error during aggregation: %s", str(e))
        raise

def save_gold_data(df: pd.DataFrame, output_filename: str) -> str:
    """
    Save the aggregated Gold DataFrame to a local file.
    
    Args:
        df (pd.DataFrame): The aggregated Gold DataFrame.
        output_filename (str): Name of the file to be saved.
        
    Returns:
        str: The full path where the file was saved.
    """
    output_dir = os.getenv("GOLD_OUTPUT_DIR", "data/gold")
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, output_filename)
        
        logger.info("Saving gold data to %s", file_path)
        df.to_csv(file_path, index=False)
        logger.info("Gold data saved successfully.")
        
        return file_path
    except Exception as e:
        logger.error("Failed to save gold data to local files: %s", str(e))
        raise

def process_gold(silver_df: pd.DataFrame) -> None:
    """
    Process the Silver data to generate and save Gold layer outputs.

    Args:
        silver_df (pd.DataFrame): Cleaned and transformed data from the Silver layer.
    """
    logger.info("Starting Gold layer processing.")
    
    try:
        regional_summary_df = create_regional_summary(silver_df)
        
        if not regional_summary_df.empty:
            save_gold_data(regional_summary_df, "regional_sales_summary.csv")
        else:
            logger.warning("No data to save for Gold layer.")
            
        logger.info("Gold layer processing completed successfully.")
    except Exception as e:
        logger.error("Gold layer processing failed: %s", str(e))
        raise