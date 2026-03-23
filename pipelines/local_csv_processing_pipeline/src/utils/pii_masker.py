import pandas as pd
import logging
from typing import List

# Configure logger for this module
logger = logging.getLogger(__name__)

def mask_pii_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Masks specified PII columns in a DataFrame by replacing their values with '***MASKED***'.

    Args:
        df (pd.DataFrame): The input DataFrame containing data, potentially with PII.
        columns (List[str]): A list of column names in the DataFrame that contain PII
                              and need to be masked.

    Returns:
        pd.DataFrame: A new DataFrame with the specified PII columns masked.
                      Returns the original DataFrame if no columns are specified or found.
    """
    if not columns:
        logger.info("No PII columns specified for masking. Returning original DataFrame.")
        return df.copy()

    df_masked = df.copy()
    masked_count = 0
    for col in columns:
        if col in df_masked.columns:
            df_masked[col] = "***MASKED***"
            logger.info(f"Column '{col}' has been masked.")
            masked_count += 1
        else:
            logger.warning(f"PII column '{col}' not found in DataFrame. Skipping masking for this column.")

    if masked_count > 0:
        logger.info(f"Successfully masked {masked_count} PII column(s) in the DataFrame.")
    else:
        logger.warning("No specified PII columns were found in the DataFrame to mask.")

    return df_masked

if __name__ == '__main__':
    # Example usage for testing purposes
    # Set up basic logging for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    data = {
        'order_id': [1, 2, 3],
        'customer_id': ['C001', 'C002', 'C003'],
        'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'amount': [100.50, 200.75, 50.00]
    }
    sample_df = pd.DataFrame(data)

    logger.info("Original DataFrame:")
    logger.info(sample_df)

    pii_columns_to_mask = ["customer_name", "email"]
    masked_df = mask_pii_columns(sample_df, pii_columns_to_mask)

    logger.info("\nDataFrame after PII masking:")
    logger.info(masked_df)

    # Test with non-existent columns
    logger.info("\nTesting with non-existent columns:")
    masked_df_non_existent = mask_pii_columns(sample_df.copy(), ["non_existent_col", "email"])
    logger.info(masked_df_non_existent)

    # Test with empty column list
    logger.info("\nTesting with empty column list:")
    masked_df_empty_list = mask_pii_columns(sample_df.copy(), [])
    logger.info(masked_df_empty_list)