import pandas as pd
import logging
import os
import hashlib
from typing import List, Dict, Any, Tuple

# Configure logging
logger = logging.getLogger(__name__)

def load_bronze_data(bronze_path: str, filename: str) -> pd.DataFrame:
    """
    Loads data from a bronze layer parquet file.

    Args:
        bronze_path (str): The base path to the bronze layer.
        filename (str): The name of the parquet file to load (e.g., 'sales.parquet').

    Returns:
        pd.DataFrame: The loaded DataFrame.

    Raises:
        FileNotFoundError: If the specified parquet file does not exist.
        Exception: For other errors during file loading.
    """
    filepath = os.path.join(bronze_path, filename)
    logger.info(f"Attempting to load bronze data from: {filepath}")
    try:
        df = pd.read_parquet(filepath)
        logger.info(f"Successfully loaded {len(df)} rows from {filename}")
        return df
    except FileNotFoundError as e:
        logger.error(f"Bronze file not found at {filepath}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading bronze data from {filepath}: {e}")
        raise

def mask_pii(df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
    """
    Masks PII columns in a DataFrame using SHA-256 hashing.

    Args:
        df (pd.DataFrame): The input DataFrame.
        pii_columns (List[str]): A list of column names to mask.

    Returns:
        pd.DataFrame: The DataFrame with PII columns masked.
    """
    df_copy = df.copy()
    for col in pii_columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str).apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
            logger.info(f"Masked PII column: {col}")
        else:
            logger.warning(f"PII column '{col}' not found in DataFrame. Skipping masking.")
    return df_copy

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the sales DataFrame by performing type casting and deduplication.
    It returns a detailed, cleaned DataFrame suitable for further aggregations
    in the gold layer, avoiding redundant aggregation.

    Args:
        df (pd.DataFrame): The input sales DataFrame.

    Returns:
        pd.DataFrame: The cleaned and transformed DataFrame (not aggregated).
    """
    logger.info("Starting cleaning and transformation of sales data.")

    initial_rows = len(df)
    logger.info(f"Initial row count: {initial_rows}")

    # Type casting
    type_mapping = {
        'quantity': 'int',
        'unit_price': 'float',
        'total_amount': 'float'
    }
    for col, dtype in type_mapping.items():
        if col in df.columns:
            # Convert to numeric, coercing errors to NaN, then fill NaN with 0 before converting to int for quantity
            if dtype == 'int':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            logger.info(f"Type casted column '{col}' to {dtype}.")
        else:
            logger.warning(f"Column '{col}' not found for type casting.")

    # Convert order_date to datetime, coercing errors to NaT
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        # Drop rows where order_date couldn't be parsed
        original_rows_after_date_cast = len(df)
        df.dropna(subset=['order_date'], inplace=True)
        if len(df) < original_rows_after_date_cast:
            logger.warning(f"Dropped {original_rows_after_date_cast - len(df)} rows due to invalid 'order_date'.")
        logger.info("Type casted column 'order_date' to datetime.")
    else:
        logger.warning("Column 'order_date' not found for type casting.")

    # Deduplication based on a composite key to preserve line items
    # Assuming 'order_id', 'product_id', and 'quantity' (or other descriptive columns)
    # form a unique line item. Adjust the subset based on actual data granularity.
    # For now, let's use a common set of columns that would identify a line item.
    # If 'product_id' is not present, 'order_id' and 'line_item_id' (if available) would be better.
    # Given the available columns, 'order_id', 'product_name', 'quantity', 'unit_price' could be a reasonable composite.
    # If 'product_name' is masked, perhaps 'product_id' should be a required column if available.
    # For this fix, let's use a combination of identifiers commonly found in sales data that represent a unique line item.
    # If 'product_id' is not available, 'product_name' (pre-masking if needed for deduplication) or just 'order_id' and time based unique identifier.
    # Let's assume 'order_id' + 'quantity' + 'unit_price' is sufficient to identify a line item.
    # This might need domain-specific adjustment.
    deduplication_subset = ['order_id', 'quantity', 'unit_price']
    if all(col in df.columns for col in deduplication_subset):
        initial_rows_before_dedupe = len(df)
        df.drop_duplicates(subset=deduplication_subset, inplace=True)
        logger.info(f"Deduplicated data based on {deduplication_subset}. Rows removed: {initial_rows_before_dedupe - len(df)}. Remaining rows: {len(df)}")
    elif 'order_id' in df.columns:
        # Fallback to order_id only if composite is not fully available, but warn about potential data loss
        logger.warning(f"Composite deduplication keys {deduplication_subset} not fully present. Deduplicating by 'order_id' alone, which may drop valid line items.")
        initial_rows_before_dedupe = len(df)
        df.drop_duplicates(subset=['order_id'], inplace=True)
        logger.info(f"Deduplicated data based on 'order_id'. Rows removed: {initial_rows_before_dedupe - len(df)}. Remaining rows: {len(df)}")
    else:
        logger.warning("Neither composite key nor 'order_id' found for deduplication. Skipping deduplication.")

    # Remove the aggregation step from clean_sales_data
    # This function should now return the detailed, cleaned DataFrame.
    # The aggregation will be handled in the gold layer.
    logger.info("Finished cleaning and transformation of sales data. Returning detailed DataFrame.")
    return df

def write_silver_data(df: pd.DataFrame, silver_path: str, filename: str) -> None:
    """
    Writes the transformed DataFrame to the silver layer as a parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        silver_path (str): The base path to the silver layer.
        filename (str): The name of the parquet file to write (e.g., 'sales_silver.parquet').

    Raises:
        Exception: For errors during file writing.
    """
    os.makedirs(silver_path, exist_ok=True)
    filepath = os.path.join(silver_path, filename)
    logger.info(f"Attempting to write silver data to: {filepath}")
    try:
        df.to_parquet(filepath, index=False)
        logger.info(f"Successfully wrote {len(df)} rows to silver layer at {filepath}")
    except Exception as e:
        logger.error(f"Error writing silver data to {filepath}: {e}")
        raise

def main() -> None:
    """
    Main function to orchestrate the silver layer transformation.
    Reads bronze data, cleans, masks PII, joins (if applicable), and writes silver data.
    """
    logger.info("Starting sales_pipeline silver layer transformation.")

    # Configuration from environment variables
    BRONZE_LAYER_PATH = os.getenv("BRONZE_LAYER_PATH", "data/bronze")
    SILVER_LAYER_PATH = os.getenv("SILVER_LAYER_PATH", "data/silver")

    SALES_BRONZE_FILENAME = "sales.parquet"
    SALES_SILVER_FILENAME = "sales_silver.parquet"

    PII_COLUMNS = ['customer_name', 'customer_email', 'product_name']

    try:
        # 1. Load bronze sales data
        sales_df = load_bronze_data(BRONZE_LAYER_PATH, SALES_BRONZE_FILENAME)

        # 2. Mask PII columns
        sales_df_masked = mask_pii(sales_df, PII_COLUMNS)

        # 3. Clean and transform sales data (type casting, deduplication)
        # This function now returns a detailed, cleaned DataFrame without aggregation.
        sales_df_silver = clean_sales_data(sales_df_masked)

        # No explicit join step mentioned beyond "join sources" but only one source 'sales' is listed
        # If multiple sources were present, this would be the place for merging/joining.

        # 4. Write silver data
        write_silver_data(sales_df_silver, SILVER_LAYER_PATH, SALES_SILVER_FILENAME)

        logger.info("Sales pipeline silver layer transformation completed successfully.")

    except FileNotFoundError:
        logger.error("Skipping silver transformation due to missing bronze file.")
    except Exception as e:
        logger.exception(f"An error occurred during the silver layer transformation: {e}")

if __name__ == "__main__":
    # Fix for Redundant logging configuration: Ensure logging.basicConfig is called only if
    # the root logger has not yet been configured (i.e., has no handlers).
    # This prevents duplicate handlers if getLogger was called implicitly before basicConfig,
    # and ensures basicConfig is effectively called only once.
    if not logging.root.handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
