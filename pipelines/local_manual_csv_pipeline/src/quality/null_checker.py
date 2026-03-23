import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: list[str]) -> dict[str, int]:
    """
    Check for null values in specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to evaluate.
        columns (list[str]): A list of column names to check for nulls.

    Returns:
        dict[str, int]: A dictionary mapping each column name to its count of null values.
    """
    logger.info("Starting null check for specified columns.")
    
    null_counts = {}
    
    for col in columns:
        if col not in df.columns:
            logger.warning("Column '%s' is not present in the DataFrame.", col)
            continue
            
        null_count = int(df[col].isna().sum())
        null_counts[col] = null_count
        
        if null_count > 0:
            logger.warning("Found %d null values in column '%s'.", null_count, col)
        else:
            logger.debug("No null values found in column '%s'.", col)

    logger.info("Completed null check.")
    return null_counts