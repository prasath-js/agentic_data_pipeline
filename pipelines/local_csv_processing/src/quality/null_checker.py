import pandas as pd
import logging
from typing import Dict, List

# Configure logging for the module
logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: List[str]) -> Dict[str, int]:
    """
    Checks specified columns in a DataFrame for null values.

    Args:
        df (pd.DataFrame): The input DataFrame.
        columns (List[str]): A list of column names to check for nulls.

    Returns:
        Dict[str, int]: A dictionary where keys are column names that contain
                        nulls and values are the count of nulls in that column.
                        Returns an empty dictionary if no nulls are found in
                        the specified columns.
    """
    null_counts: Dict[str, int] = {}
    for col in columns:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame. Skipping null check for this column.")
            continue
        
        null_count = df[col].isnull().sum()
        if null_count > 0:
            null_counts[col] = null_count
            logger.warning(f"Null values detected in column '{col}': {null_count} occurrences.")
    
    if not null_counts:
        logger.info("No null values found in the specified columns.")
    
    return null_counts