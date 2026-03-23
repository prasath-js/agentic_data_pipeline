import pandas as pd
import logging
from typing import Dict
from src.utils.pii_masker import mask_pii

# Configure logging for the silver layer
logger = logging.getLogger(__name__)

def transform_silver(bronze_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transforms raw bronze data into a clean, integrated silver layer DataFrame.

    This function performs the following steps:
    1. Retrieves the 'local_csv_data' DataFrame from the bronze_data dictionary.
    2. Masks specified PII columns ('customer_name', 'email').
    3. Converts the 'order_date' column from 'DD/MM/YYYY' to ISO format (YYYY-MM-DD).
    4. Removes rows with null values in critical columns ('order_id', 'customer_id', 'amount', 'order_date').
    5. Filters out invalid rows where 'amount' is zero or negative.

    Args:
        bronze_data (Dict[str, pd.DataFrame]): A dictionary where keys are source names
                                                and values are their respective raw Bronze DataFrames.

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame ready for the Gold layer.
    """
    logger.info("Starting silver layer transformation.")

    if "local_csv_data" not in bronze_data:
        logger.error("Required source 'local_csv_data' not found in bronze_data.")
        raise ValueError("Missing 'local_csv_data' in bronze_data for silver transformation.")

    df_local_csv = bronze_data["local_csv_data"].copy()
    logger.debug(f"Initial 'local_csv_data' DataFrame shape: {df_local_csv.shape}")

    # 1. PII Masking
    pii_columns = ["customer_name", "email"]
    for col in pii_columns:
        if col in df_local_csv.columns:
            df_local_csv[col] = df_local_csv[col].apply(mask_pii)
            logger.info(f"PII masked column: {col}")
        else:
            logger.warning(f"PII column '{col}' not found in 'local_csv_data'. Skipping masking.")

    # 2. Date Format Resolution
    date_column = "order_date"
    if date_column in df_local_csv.columns:
        try:
            # Convert 'DD/MM/YYYY' to datetime objects, then format to 'YYYY-MM-DD'
            df_local_csv[date_column] = pd.to_datetime(df_local_csv[date_column], format="%d/%m/%Y", errors="coerce")
            df_local_csv[date_column] = df_local_csv[date_column].dt.strftime("%Y-%m-%d")
            logger.info(f"Fixed date format for column '{date_column}' to YYYY-MM-DD.")
        except Exception as e:
            logger.error(f"Error converting date column '{date_column}': {e}")
            # If conversion fails for some rows, they might become NaT. Handle this during null removal.
    else:
        logger.warning(f"Date column '{date_column}' not found in 'local_csv_data'. Skipping date format fix.")

    # 3. Remove Nulls in Critical Columns
    critical_columns = ["order_id", "customer_id", "amount", "order_date"]
    original_rows_before_null_check = df_local_csv.shape[0]
    df_local_csv.dropna(subset=critical_columns, inplace=True)
    rows_removed_by_null_check = original_rows_before_null_check - df_local_csv.shape[0]
    if rows_removed_by_null_check > 0:
        logger.warning(f"Removed {rows_removed_by_null_check} rows due to nulls in critical columns: {critical_columns}.")
    logger.debug(f"DataFrame shape after null removal: {df_local_csv.shape}")

    # Ensure 'amount' is numeric for filtering
    if 'amount' in df_local_csv.columns:
        try:
            df_local_csv['amount'] = pd.to_numeric(df_local_csv['amount'], errors='coerce')
            # Drop rows where amount couldn't be converted to numeric (now NaN)
            df_local_csv.dropna(subset=['amount'], inplace=True)
        except Exception as e:
            logger.error(f"Error converting 'amount' column to numeric: {e}")
            # If conversion fails for the whole column, subsequent filtering might error or be ineffective.

    # 4. Filter Invalid Rows (amount <= 0)
    original_rows_before_amount_filter = df_local_csv.shape[0]
    df_local_csv = df_local_csv[df_local_csv['amount'] > 0]
    rows_removed_by_amount_filter = original_rows_before_amount_filter - df_local_csv.shape[0]
    if rows_removed_by_amount_filter > 0:
        logger.warning(f"Removed {rows_removed_by_amount_filter} rows where 'amount' was 0 or negative.")
    logger.debug(f"DataFrame shape after invalid amount filtering: {df_local_csv.shape}")

    logger.info(f"Silver layer transformation completed. Final DataFrame shape: {df_local_csv.shape}")
    return df_local_csv
