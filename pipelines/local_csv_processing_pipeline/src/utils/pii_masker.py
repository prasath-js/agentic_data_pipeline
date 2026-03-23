import logging
import pandas as pd
from typing import List

logger = logging.getLogger(__name__)

def mask_pii_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Masks Personally Identifiable Information (PII) columns in a DataFrame.
    
    Replaces all values in the specified PII columns with '***MASKED***'.
    Logs the columns that were successfully masked.

    Args:
        df (pd.DataFrame): The input DataFrame containing potential PII data.
        columns (List[str]): A list of column names to be masked.

    Returns:
        pd.DataFrame: A DataFrame with the specified columns masked.
    """
    logger.info("Starting PII masking process.")
    
    df_masked = df.copy()
    masked_cols = []

    for col in columns:
        if col in df_masked.columns:
            df_masked[col] = "***MASKED***"
            masked_cols.append(col)
        else:
            logger.warning("Column '%s' not found in DataFrame. Skipping masking for this column.", col)

    if masked_cols:
        logger.info("Successfully masked PII columns: %s", ", ".join(masked_cols))
    else:
        logger.info("No PII columns were masked.")

    return df_masked