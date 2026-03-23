import logging
import pandas as pd

logger = logging.getLogger(__name__)


def mask_pii_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Mask Personally Identifiable Information (PII) in the given DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing potential PII.
        columns (list[str]): A list of column names to mask.

    Returns:
        pd.DataFrame: A DataFrame with the specified PII columns masked.
    """
    logger.info("Starting PII masking process.")
    
    if df is None or df.empty:
        logger.warning("Input DataFrame is empty or None. Returning as is.")
        return df

    # Create a copy to avoid SettingWithCopyWarning and mutating original inputs
    masked_df = df.copy()
    masked_count = 0

    for col in columns:
        if col in masked_df.columns:
            masked_df[col] = "***MASKED***"
            masked_count += 1
            logger.debug(f"Masked PII column: {col}")
        else:
            logger.warning(f"Requested PII column '{col}' not found in DataFrame. Skipping.")

    logger.info(f"Successfully masked {masked_count} columns.")
    
    return masked_df