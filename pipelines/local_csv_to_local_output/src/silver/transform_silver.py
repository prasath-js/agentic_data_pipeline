import pandas as pd
import logging
from typing import Dict, Any

from src.utils.pii_masker import mask_pii_columns
from src.quality.null_checker import check_nulls
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def transform_silver_data(bronze_df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """
    Applies a series of transformations to the bronze DataFrame to create the silver layer.
    This includes PII masking, date format standardization, null removal, and row filtering.

    Args:
        bronze_df (pd.DataFrame): The raw DataFrame from the bronze layer.
        settings (Settings): Application settings containing configuration for silver transformations.

    Returns:
        pd.DataFrame: The transformed DataFrame for the silver layer.
    """
    logger.info("Starting Silver layer transformations.")

    if bronze_df.empty:
        logger.warning("Bronze DataFrame is empty. Skipping Silver transformations.")
        return pd.DataFrame()

    silver_df = bronze_df.copy()

    # 1. Date Format Standardization
    date_cols = ["close_date", "transaction_date"]
    for col in date_cols:
        if col in silver_df.columns:
            logger.info(f"Attempting to convert column '{col}' to datetime.")
            # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
            silver_df[col] = pd.to_datetime(silver_df[col], errors='coerce', format='%Y-%m-%d')
            if silver_df[col].isnull().any():
                null_dates_count = silver_df[col].isnull().sum()
                logger.warning(
                    f"Found {null_dates_count} invalid date entries in '{col}' after conversion. "
                    "These will be treated as NaT."
                )
        else:
            logger.warning(f"Date column '{col}' not found in DataFrame.")

    # 2. PII Masking
    pii_columns = settings.silver_settings.pii_columns_to_mask
    if pii_columns:
        logger.info(f"Masking PII columns: {pii_columns}")
        silver_df = mask_pii_columns(silver_df, pii_columns)
    else:
        logger.info("No PII columns configured for masking.")

    # 3. Remove Nulls in Critical Columns
    critical_null_columns = settings.silver_settings.critical_null_columns
    if critical_null_columns:
        initial_rows = len(silver_df)
        null_check_result, null_counts = check_nulls(silver_df, critical_null_columns)
        if not null_check_result:
            logger.warning(f"Nulls detected in critical columns: {null_counts}")
            silver_df.dropna(subset=critical_null_columns, inplace=True)
            rows_after_null_removal = len(silver_df)
            logger.info(
                f"Removed {initial_rows - rows_after_null_removal} rows due to nulls "
                f"in critical columns: {critical_null_columns}. Remaining rows: {rows_after_null_removal}"
            )
        else:
            logger.info("No nulls detected in critical columns.")
    else:
        logger.info("No critical null columns configured for checking/removal.")

    # 4. Filter Invalid Rows (e.g., amount <= 0)
    if "amount" in silver_df.columns:
        initial_rows = len(silver_df)
        min_valid_amount = settings.silver_settings.min_valid_amount
        silver_df = silver_df[silver_df["amount"] > min_valid_amount]
        rows_after_filter = len(silver_df)
        if initial_rows - rows_after_filter > 0:
            logger.info(
                f"Removed {initial_rows - rows_after_filter} rows where 'amount' was <= {min_valid_amount}. "
                f"Remaining rows: {rows_after_filter}"
            )
        else:
            logger.info("No rows filtered based on 'amount' value.")
    else:
        logger.warning("Column 'amount' not found for filtering invalid rows.")

    logger.info(f"Silver layer transformations completed. Final DataFrame has {len(silver_df)} rows.")
    return silver_df

if __name__ == '__main__':
    # This block is for demonstration/testing purposes when running this file directly.
    from src.config.logging_config import setup_logging
    import os
    setup_logging()
    settings = Settings()

    # Create a dummy bronze DataFrame for testing
    dummy_bronze_data = {
        "opportunity_id": [1, 2, 3, 4, 5],
        "account_id": ["ACC001", "ACC002", "ACC001", "ACC003", "ACC004"],
        "value": [1000, 2500, 1500, 0, 500],
        "close_date": ["2023-01-15", "2023-02-20", "invalid-date", "2023-03-01", "2023-04-05"],
        "stage": ["Closed Won", "Open", "Closed Lost", "Open", "Closed Won"],
        "transaction_id": ["T001", "T002", "T003", None, "T005"],
        "customer_id": ["CUST001", "CUST002", "CUST001", "CUST003", "CUST004"],
        "quantity": [10, 5, 8, 0, 2],
        "amount": [950.50, 2400.00, None, -50.00, 480.00],
        "transaction_date": ["2023-01-10", "2023-02-18", "2023-01-22", "invalid-date", "2023-04-01"]
    }
    bronze_df_test = pd.DataFrame(dummy_bronze_data)
    logger.info("Initial Bronze DataFrame:\n%s", bronze_df_test)

    # Override settings for direct run demonstration
    settings.silver_settings.pii_columns_to_mask = ["customer_id", "account_id"]
    settings.silver_settings.critical_null_columns = ["opportunity_id", "amount", "transaction_id"]
    settings.silver_settings.min_valid_amount = 0.0

    silver_df_result = transform_silver_data(bronze_df_test, settings)
    logger.info("Silver DataFrame head:\n%s", silver_df_result.head())
    logger.info("Silver DataFrame info:\n%s", silver_df_result.info())
    logger.info("Silver DataFrame dtypes:\n%s", silver_df_result.dtypes)