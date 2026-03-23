import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_silver(df: pd.DataFrame, pii_columns: list[str]) -> pd.DataFrame:
    """
    Transforms raw bronze data into standardized silver data.
    
    Operations applied:
    - Masks specified PII columns with '***MASKED***'.
    - Converts 'order_date' from DD/MM/YYYY format to ISO 8601 (YYYY-MM-DD).
    - Removes rows with null values in critical columns ('order_id', 'customer_id').
    - Filters out invalid rows where 'amount' <= 0.
    
    Args:
        df (pd.DataFrame): The raw bronze DataFrame.
        pii_columns (list[str]): List of column names to mask.
        
    Returns:
        pd.DataFrame: The cleaned and transformed silver DataFrame.
    """
    logger.info("Starting silver transformation process.")
    
    # Create a copy to avoid SettingWithCopyWarning
    df_transformed = df.copy()
    
    # 1. Mask PII Columns
    for col in pii_columns:
        if col in df_transformed.columns:
            df_transformed[col] = "***MASKED***"
            logger.info("Masked PII column: %s", col)
        else:
            logger.warning("Requested PII column '%s' not found in DataFrame.", col)
            
    # 2. Fix Date Conflicts (DD/MM/YYYY -> ISO 8601)
    if 'order_date' in df_transformed.columns:
        try:
            # Parse DD/MM/YYYY and convert to standard YYYY-MM-DD string
            df_transformed['order_date'] = pd.to_datetime(
                df_transformed['order_date'], 
                format='%d/%m/%Y', 
                errors='coerce'
            ).dt.strftime('%Y-%m-%d')
            logger.info("Standardized 'order_date' to ISO format.")
        except Exception as e:
            logger.error("Error formatting 'order_date': %s", str(e))
    
    # 3. Remove Nulls in Critical Columns
    critical_columns = ['order_id', 'customer_id']
    existing_critical_cols = [c for c in critical_columns if c in df_transformed.columns]
    
    if existing_critical_cols:
        initial_row_count = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=existing_critical_cols)
        dropped_null_rows = initial_row_count - len(df_transformed)
        if dropped_null_rows > 0:
            logger.info("Removed %d rows with nulls in critical columns %s.", 
                        dropped_null_rows, existing_critical_cols)
    
    # 4. Filter Invalid Rows (amount <= 0)
    if 'amount' in df_transformed.columns:
        # Ensure the column is numeric so we can filter accurately
        df_transformed['amount'] = pd.to_numeric(df_transformed['amount'], errors='coerce')
        
        initial_row_count = len(df_transformed)
        # Keep rows where amount > 0, treating NaNs as invalid
        df_transformed = df_transformed[df_transformed['amount'] > 0]
        dropped_invalid_rows = initial_row_count - len(df_transformed)
        if dropped_invalid_rows > 0:
            logger.info("Filtered out %d rows with invalid amount (<= 0 or NaN).", 
                        dropped_invalid_rows)
            
    logger.info("Silver transformation complete. Final row count: %d", len(df_transformed))
    return df_transformed