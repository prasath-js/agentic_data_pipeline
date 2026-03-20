import pandas as pd
import logging

# Configure logging for the module
logger = logging.getLogger(__name__)

def mask_pii_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Masks specified PII columns in a DataFrame by replacing their values with '***MASKED***'.

    Args:
        df (pd.DataFrame): The input DataFrame containing PII columns.
        columns (list[str]): A list of column names to be masked.

    Returns:
        pd.DataFrame: A new DataFrame with the specified PII columns masked.
                      Returns the original DataFrame if no columns are specified or found.
    """
    if not columns:
        logger.info("No PII columns specified for masking.")
        return df.copy()

    df_masked = df.copy()
    masked_count = 0
    for col in columns:
        if col in df_masked.columns:
            df_masked[col] = '***MASKED***'
            logger.info(f"Column '{col}' has been masked.")
            masked_count += 1
        else:
            logger.warning(f"PII column '{col}' not found in the DataFrame. Skipping masking for this column.")

    if masked_count > 0:
        logger.info(f"Successfully masked {masked_count} PII column(s).")
    else:
        logger.info("No PII columns were actually masked (either not specified or not found).")

    return df_masked