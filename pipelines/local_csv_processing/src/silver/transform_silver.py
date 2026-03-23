import pandas as pd
import logging
from typing import Dict, Any

from config.logging_config import configure_logging
from config.settings import get_settings # Added: Import get_settings
from utils.pii_masker import mask_pii_dataframe

configure_logging()
logger = logging.getLogger(__name__)

def transform_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a series of transformations to the bronze DataFrame to create a silver-layer DataFrame.
    This includes date format correction, PII masking, handling nulls in critical columns,
    and filtering invalid rows.

    Args:
        bronze_df (pd.DataFrame): The raw DataFrame ingested from the bronze layer,
                                  expected to contain columns:
                                  'order_id', 'customer_id', 'customer_name', 'email',
                                  'amount', 'status', 'region', 'order_date'.

    Returns:
        pd.DataFrame: A transformed DataFrame suitable for the silver layer,
                      with PII masked, dates standardized, critical nulls removed,
                      and invalid 'amount' rows filtered.
    """
    logger.info("Starting silver layer transformations.")

    settings = get_settings() # Added: Get settings object

    silver_df = bronze_df.copy()

    # --- 1. Date Format Conflicts Resolution ---
    # Non-ISO date format detected: {'input_csv_folder': 'DD/MM/YYYY'}
    date_column: str = "order_date" # Kept: 'order_date' is a specific column name, not a configurable setting
    expected_input_format: str = settings.INPUT_CSV_DATE_FORMAT # Changed: Use settings for date format
    if date_column in silver_df.columns:
        logger.info(f"Attempting to fix date format for '{date_column}' from 'DD/MM/YYYY' to ISO format.")
        try:
            # Convert to datetime objects first, inferring format if possible, then to ISO string
            # Using errors='coerce' will turn unparseable dates into NaT
            silver_df[date_column] = pd.to_datetime(silver_df[date_column], format=expected_input_format, errors='coerce')
            # Convert back to ISO 8601 string format (YYYY-MM-DD) or leave as NaT
            silver_df[date_column] = silver_df[date_column].dt.strftime('%Y-%m-%d')
            logger.info(f"Successfully converted '{date_column}' to ISO format (YYYY-MM-DD).")
        except Exception as e:
            logger.error(f"Error converting '{date_column}' to datetime: {e}")
            # If conversion fails, it's safer to drop the column or leave as is if no clear conversion path
            # For this pipeline, we'll keep it as is, but log the error.
    else:
        logger.warning(f"Date column '{date_column}' not found in DataFrame.")

    # --- 2. PII Masking ---
    pii_columns: list[str] = settings.PII_COLUMNS_TO_MASK # Changed: Use settings for PII columns
    logger.info(f"Masking PII columns: {pii_columns}")
    silver_df = mask_pii_dataframe(silver_df, pii_columns, "***MASKED***")
    logger.info("PII masking completed.")

    # --- 3. Remove Nulls in Critical Columns ---
    critical_columns: list[str] = settings.CRITICAL_NULL_CHECK_COLUMNS # Changed: Use settings for critical null check columns
    # Filter only columns that actually exist in the DataFrame
    existing_critical_columns = [col for col in critical_columns if col in silver_df.columns]

    if existing_critical_columns:
        initial_row_count: int = len(silver_df)
        silver_df.dropna(subset=existing_critical_columns, inplace=True)
        rows_dropped_nulls: int = initial_row_count - len(silver_df)
        if rows_dropped_nulls > 0:
            logger.warning(
                f"Dropped {rows_dropped_nulls} rows due to nulls in critical columns: {existing_critical_columns}"
            )
        else:
            logger.info("No rows dropped due to nulls in critical columns.")
    else:
        logger.warning("No critical columns found for null checking.")

    # --- 4. Filter Invalid Rows (amount is 0 or negative) ---
    amount_column: str = "amount"
    if amount_column in silver_df.columns:
        initial_row_count_filter: int = len(silver_df)
        
        # Ensure 'amount' column is numeric before filtering
        silver_df[amount_column] = pd.to_numeric(silver_df[amount_column], errors='coerce')
        
        # Drop rows where 'amount' became NaN during coercion (or was already NaN)
        # This handles cases where amount might not be a number
        silver_df.dropna(subset=[amount_column], inplace=True)
        
        # Now filter for positive amounts
        silver_df = silver_df[silver_df[amount_column] > 0]
        rows_dropped_invalid_amount: int = initial_row_count_filter - len(silver_df)
        if rows_dropped_invalid_amount > 0:
            logger.warning(
                f"Dropped {rows_dropped_invalid_amount} rows where '{amount_column}' was non-numeric, 0 or negative."
            )
        else:
            logger.info(f"No rows dropped due to invalid '{amount_column}' values.")
    else:
        logger.warning(f"Amount column '{amount_column}' not found in DataFrame for filtering.")

    logger.info("Silver layer transformations completed.")
    return silver_df

if __name__ == '__main__':
    # This block is for demonstration/testing purposes when running the script directly
    logger.info("Running transform_silver.py in standalone test mode.")

    # Create a dummy DataFrame mimicking bronze layer output
    data: Dict[str, list[Any]] = {
        "order_id": ["O001", "O002", "O003", "O004", "O005", "O006", "O007"],
        "customer_id": ["C1", "C2", "C1", "C3", "C2", "C4", "C5"],
        "customer_name": ["Alice Smith", "Bob Johnson", "Alice Smith", "Charlie Brown", "Eve Davis", "Frank White", "Grace Green"],
        "email": ["alice@example.com", "bob@example.com", "alice@example.com", "charlie@example.com", "eve@example.com", "frank@example.com", "grace@example.com"],
        "amount": [100.50, 200.00, 50.75, -10.00, 0.00, None, 75.25],
        "status": ["completed", "pending", "completed", "cancelled", "completed", "pending", "completed"],
        "region": ["East", "West", "East", "North", "South", "West", None],
        "order_date": ["15/03/2023", "20/04/2023", "01/01/2023", "10/02/2023", "abc", "25/12/2023", None]
    }
    bronze_test_df = pd.DataFrame(data)
    logger.info("Bronze test DataFrame created:\n%s", bronze_test_df.to_string())

    # Apply silver transformations
    silver_test_df = transform_silver(bronze_test_df)
    logger.info("Silver transformed DataFrame:\n%s", silver_test_df.to_string())