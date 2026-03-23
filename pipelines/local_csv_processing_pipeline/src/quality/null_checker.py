import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: list[str]) -> dict[str, int]:
    """
    Check for null values in the specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to evaluate.
        columns (list[str]): A list of column names to check for null values.

    Returns:
        dict[str, int]: A dictionary mapping each column name to its null count.
    """
    null_counts = {}
    
    for col in columns:
        if col not in df.columns:
            logger.warning("Column '%s' is not present in the DataFrame. Skipping null check.", col)
            continue
            
        count = int(df[col].isnull().sum())
        null_counts[col] = count
        
        if count > 0:
            logger.warning("Quality Check Warning: Found %d null values in column '%s'.", count, col)
            
    return null_counts