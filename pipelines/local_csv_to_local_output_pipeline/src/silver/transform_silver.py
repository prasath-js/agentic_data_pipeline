import pandas as pd
import logging
from typing import Dict, Any, List

from src.utils.pii_masker import mask_pii_columns
from src.quality.null_checker import check_for_nulls

logger = logging.getLogger(__name__)

def transform_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a series of transformations to the bronze DataFrame to create the silver layer.

    This includes:
    - Ensuring correct date formats for 'close_date' and 'transaction_date'.
    - Removing rows with nulls in critical columns ('opportunity_id', 'account_id', 'amount').
    - Filtering out invalid rows (e.g., 'amount' <= 0).
    - Masking PII columns (though none are detected in this pipeline, it's included for completeness).

    Args:
        df_bronze (pd.DataFrame): The raw DataFrame from the bronze layer.

    Returns:
        pd.DataFrame: The transformed DataFrame for the silver layer.
    """
    logger.info("Starting silver layer transformations.")
    initial_row_count = len(df_bronze)
    df_silver = df_bronze.copy()

    # 1. Date Format Conflicts (Ensuring ISO format)
    date_columns = ['close_date', 'transaction_date']
    for col in date_columns:
        if col in df_silver.columns:
            logger.debug(f"Converting column '{col}' to datetime format.")
            # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
            df_silver[col] = pd.to_datetime(df_silver[col], errors='coerce')
            # Optionally, remove rows where date conversion failed if critical
            # For this pipeline, we'll let null removal handle NaT if the column is critical
            if df_silver[col].isnull().any():
                logger.warning(f"Nulls introduced in '{col}' after date conversion. These will be handled by null removal if '{col}' is a critical column.")
        else:
            logger.warning(f"Date column '{col}' not found in bronze DataFrame. Skipping date conversion for this column.")

    # 2. Remove nulls in critical columns
    critical_columns = ['opportunity_id', 'account_id', 'amount']
    # Check for nulls before removal
    null_report = check_for_nulls(df_silver, critical_columns)
    if null_report:
        logger.warning(f"Nulls detected in critical columns before removal: {null_report}")

    df_silver.dropna(subset=critical_columns, inplace=True)
    rows_after_null_removal = len(df_silver)
    logger.info(f"Removed {initial_row_count - rows_after_null_removal} rows due to nulls in critical columns. Remaining rows: {rows_after_null_removal}")

    # 3. Filter invalid rows (e.g., amount <= 0)
    if 'amount' in df_silver.columns:
        initial_rows_before_amount_filter = len(df_silver)
        df_silver = df_silver[df_silver['amount'] > 0]
        rows_after_amount_filter = len(df_silver)
        logger.info(f"Removed {initial_rows_before_amount_filter - rows_after_amount_filter} rows due to 'amount' <= 0. Remaining rows: {rows_after_amount_filter}")
    else:
        logger.warning("'amount' column not found for filtering invalid rows.")

    # 4. Mask all PII columns (No PII detected in context, so this will be a no-op)
    # If PII were detected, e.g., 'customer_name', 'email', they would be listed here.
    pii_columns: List[str] = [] # Based on context: "pii_detected": []
    if pii_columns:
        df_silver = mask_pii_columns(df_silver, pii_columns)
        logger.info(f"Applied PII masking to columns: {pii_columns}")
    else:
        logger.info("No PII columns detected for masking in this pipeline.")

    final_row_count = len(df_silver)
    logger.info(f"Silver layer transformations complete. Initial rows: {initial_row_count}, Final rows: {final_row_count}")

    return df_silver

if __name__ == '__main__':
    # Example usage for standalone testing
    # Create a dummy bronze DataFrame
    data = {
        'opportunity_id': [1, 2, 3, 4, 5, 6, 7],
        'account_id': [101, 102, 101, 103, None, 101, 102],
        'value': [1000.00, 500.00, 200.00, 750.00, 300.00, 0.00, 400.00],
        'close_date': ['2023-01-15', '2023-02-20', '2023-03-01', '2023-04-10', 'invalid-date', '2023-05-01', '2023-06-10'],
        'stage': ['Closed Won', 'Open', 'Closed Lost', 'Open', 'Open', 'Closed Won', 'Open'],
        'transaction_id': ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7'],
        'customer_id': ['C1', 'C2', 'C1', 'C3', 'C4', 'C1', 'C2'],
        'quantity': [10, 5, 2, 8, 3, 0, 4],
        'amount': [100.00, 50.00, 20.00, 75.00, None, -10.00, 40.00],
        'transaction_date': ['2023-01-10', '2023-02-15', '2023-02-28', '2023-04-05', '2023-04-20', '2023-04-25', '2023-06-05']
    }
    bronze_df_test = pd.DataFrame(data)

    # Configure logging for testing
    os.environ["LOG_LEVEL"] = "INFO"
    from src.config.logging_config import configure_logging
    configure_logging()

    print("--- Bronze DataFrame (Initial) ---")
    print(bronze_df_test)
    print(f"Initial row count: {len(bronze_df_test)}")

    silver_df_test = transform_silver(bronze_df_test)

    print("\n--- Silver DataFrame (Transformed) ---")
    print(silver_df_test)
    print(f"Final row count: {len(silver_df_test)}")
    print("\nData Types in Silver DataFrame:")
    print(silver_df_test.dtypes)

    expected_rows = 4 # (7 initial - 1 (account_id is null) - 1 (amount is null) - 1 (amount <= 0) = 4)
    assert len(silver_df_test) == expected_rows, f"Expected {expected_rows} rows, got {len(silver_df_test)}"
    assert silver_df_test['close_date'].dtype == 'datetime64[ns]', "close_date not datetime"
    assert silver_df_test['transaction_date'].dtype == 'datetime64[ns]', "transaction_date not datetime"
    assert not silver_df_test['amount'].isnull().any(), "Amount column still has nulls"
    assert (silver_df_test['amount'] > 0).all(), "Amount column has non-positive values"
    print("\nAssertions passed for test data.")