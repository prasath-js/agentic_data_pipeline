import pandas as pd
import logging
from typing import Dict

# Set up logging for the module
logger = logging.getLogger(__name__)

def transform_silver(bronze_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Applies silver-layer transformations to the bronze DataFrame(s).

    This function performs the following operations:
    1. Retrieves the 'local_csv_input' DataFrame from the bronze data.
    2. Fixes non-ISO date formats in 'close_date' and 'transaction_date' columns
       to a standardized ISO (YYYY-MM-DD) format, converting them to datetime objects
       and then back to string representation if original format was non-ISO.
    3. Removes rows with null values in critical columns such as
       'opportunity_id', 'account_id', 'value', 'amount', 'transaction_id', 'customer_id'.
    4. Filters out invalid rows where the 'amount' is less than or equal to zero.

    Args:
        bronze_data (Dict[str, pd.DataFrame]): A dictionary where keys are source names
                                               and values are their respective bronze
                                               DataFrames. Expected to contain
                                               'local_csv_input'.

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame ready for the gold layer.
                      Returns an empty DataFrame if the required source is not found
                      or if no data remains after transformations.
    """
    logger.info("Starting silver layer transformation for local_csv_processing.")

    # Retrieve the specific source DataFrame
    if "local_csv_input" not in bronze_data:
        logger.error("Required source 'local_csv_input' not found in bronze_data.")
        return pd.DataFrame()

    df = bronze_data["local_csv_input"].copy()
    initial_rows = len(df)
    logger.info(f"Initial rows in 'local_csv_input' DataFrame: {initial_rows}")

    if df.empty:
        logger.warning("Bronze DataFrame for 'local_csv_input' is empty. Skipping transformations.")
        return pd.DataFrame()

    # --- 1. Fix Date Format Conflicts ---
    date_columns = ["close_date", "transaction_date"]
    for col in date_columns:
        if col in df.columns:
            logger.debug(f"Attempting to standardize date column: '{col}'")
            # Convert to datetime, coercing errors to NaT (Not a Time)
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Convert back to YYYY-MM-DD string format
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            logger.debug(f"Standardized date column '{col}' to YYYY-MM-DD format.")
        else:
            logger.warning(f"Date column '{col}' not found in DataFrame.")

    # --- 2. Remove Nulls in Critical Columns ---
    critical_columns = ["opportunity_id", "account_id", "value", "amount", "transaction_id", "customer_id"]
    # Filter for columns that actually exist in the DataFrame
    existing_critical_columns = [col for col in critical_columns if col in df.columns]

    if existing_critical_columns:
        rows_before_null_drop = len(df)
        df.dropna(subset=existing_critical_columns, inplace=True)
        rows_after_null_drop = len(df)
        logger.info(
            f"Removed {rows_before_null_drop - rows_after_null_drop} rows due to nulls "
            f"in critical columns: {', '.join(existing_critical_columns)}. "
            f"Remaining rows: {rows_after_null_drop}"
        )
    else:
        logger.warning(f"None of the specified critical columns {critical_columns} were found for null removal.")


    # --- 3. Filter Invalid Rows (amount <= 0) ---
    if 'amount' in df.columns:
        # Ensure 'amount' is numeric, coercing errors to NaN
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        rows_before_amount_filter = len(df)
        df = df[df['amount'] > 0]
        rows_after_amount_filter = len(df)
        logger.info(
            f"Removed {rows_before_amount_filter - rows_after_amount_filter} rows "
            f"where 'amount' was <= 0 or non-numeric. "
            f"Remaining rows: {rows_after_amount_filter}"
        )
    else:
        logger.warning("Column 'amount' not found for filtering invalid rows.")

    final_rows = len(df)
    logger.info(f"Silver layer transformation completed. Final rows: {final_rows}")
    logger.info(f"Total rows processed: {initial_rows}, rows remaining after silver transformation: {final_rows}")

    if df.empty:
        logger.warning("No data remaining after silver layer transformations.")

    return df