# src/silver/transform_silver.py
import pandas as pd
import logging
from typing import Dict, Any, List
from src.utils.pii_masker import mask_dataframe_columns
from src.quality.null_checker import check_and_handle_nulls
from src.quality.row_count_validator import validate_row_count

logger = logging.getLogger(__name__)

def transform_silver(bronze_df: pd.DataFrame, pipeline_metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Applies a series of transformations to the bronze layer DataFrame to create
    a clean and standardized silver layer DataFrame.

    Transformations include:
    - Fixing date format conflicts.
    - Removing nulls in critical columns.
    - Filtering invalid rows (e.g., non-positive amounts/quantities).
    - Masking PII columns (if any are detected and configured).

    Args:
        bronze_df (pd.DataFrame): The raw DataFrame from the bronze layer.
        pipeline_metadata (Dict[str, Any]): Dictionary containing metadata about the pipeline,
                                             including PII detection and column information.

    Returns:
        pd.DataFrame: The transformed DataFrame ready for the gold layer.
    """
    if bronze_df.empty:
        logger.warning("Bronze DataFrame is empty, skipping silver transformations.")
        return pd.DataFrame()

    initial_row_count = len(bronze_df)
    logger.info(f"Starting silver transformation with {initial_row_count} rows.")

    silver_df = bronze_df.copy()

    # --- 1. Fix date format conflicts ---
    # Detected date columns in context: 'close_date', 'transaction_date'
    date_columns = ['close_date', 'transaction_date']
    for col in date_columns:
        if col in silver_df.columns:
            try:
                # Assuming ISO (YYYY-MM-DD) as per context, pandas to_datetime is robust
                silver_df[col] = pd.to_datetime(silver_df[col], errors='coerce')
                logger.debug(f"Converted column '{col}' to datetime format.")
            except Exception as e:
                logger.error(f"Error converting '{col}' to datetime: {e}", exc_info=True)

    # --- 2. Remove nulls in critical columns ---
    # Identify critical columns based on the profile summary
    critical_columns = [
        "opportunity_id", "account_id", "value", "amount", "quantity"
    ]
    # Filter to only include columns actually present in the DataFrame
    critical_columns_present = [col for col in critical_columns if col in silver_df.columns]

    if critical_columns_present:
        silver_df = check_and_handle_nulls(
            df=silver_df,
            columns=critical_columns_present,
            strategy="drop" # Drop rows where critical columns are null
        )
        logger.info(f"After handling nulls in critical columns, {len(silver_df)} rows remaining.")
    else:
        logger.warning("No critical columns found for null checking in silver transformation.")


    # --- 3. Filter invalid rows (amount <= 0 etc) ---
    numeric_columns_to_filter = {
        "value": 0,
        "amount": 0,
        "quantity": 0
    }
    for col, threshold in numeric_columns_to_filter.items():
        if col in silver_df.columns:
            original_count = len(silver_df)
            # Ensure column is numeric before filtering
            silver_df[col] = pd.to_numeric(silver_df[col], errors='coerce')
            silver_df = silver_df[silver_df[col] > threshold].copy()
            rows_filtered = original_count - len(silver_df)
            if rows_filtered > 0:
                logger.info(f"Filtered {rows_filtered} rows where '{col}' was not greater than {threshold}.")
        else:
            logger.debug(f"Column '{col}' not found for filtering non-positive values.")

    # --- 4. Mask all PII columns ---
    # Based on the context, pii_detected is empty, so this will effectively do nothing
    # but the function call is kept for completeness and future extensibility.
    pii_columns: List[str] = pipeline_metadata.get("profiles_summary", {}).get("csv_input", {}).get("pii_detected", [])
    if pii_columns:
        silver_df = mask_dataframe_columns(silver_df, pii_columns)
        logger.info(f"Masked PII columns: {', '.join(pii_columns)}")
    else:
        logger.info("No PII columns detected for masking in this pipeline.")

    final_row_count = len(silver_df)
    logger.info(f"Silver transformation completed. Rows from {initial_row_count} to {final_row_count}.")

    # --- 5. Validate row count after transformations ---
    # Example validation: ensure at least 80% of rows remain after cleaning
    min_expected_rows = int(initial_row_count * 0.8)
    if not validate_row_count(silver_df, min_rows=min_expected_rows):
        logger.warning(f"Row count after silver transformation ({final_row_count}) is below "
                       f"the expected minimum of {min_expected_rows}.")

    return silver_df