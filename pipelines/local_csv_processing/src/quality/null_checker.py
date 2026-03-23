import pandas as pd
import logging
from typing import Dict, List

# Configure logging for the quality module
logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: List[str]) -> Dict[str, int]:
    """
    Checks for null values in specified columns of a DataFrame and logs warnings
    if any nulls are found.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        columns (List[str]): A list of column names to check for nulls.

    Returns:
        Dict[str, int]: A dictionary where keys are column names and values are
                        the count of nulls found in that column. Only includes
                        columns with nulls.
    """
    if not isinstance(df, pd.DataFrame):
        logger.error("Invalid input: df must be a pandas DataFrame.")
        raise TypeError("df must be a pandas DataFrame.")
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        logger.error("Invalid input: columns must be a list of strings.")
        raise TypeError("columns must be a list of strings.")

    null_counts: Dict[str, int] = {}
    for col in columns:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame. Skipping null check for this column.")
            continue
        
        num_nulls = df[col].isnull().sum()
        if num_nulls > 0:
            null_counts[col] = num_nulls
            logger.warning(f"Null values detected in column '{col}': {num_nulls} nulls found.")
        else:
            logger.info(f"No null values found in column '{col}'.")
            
    return null_counts

if __name__ == '__main__':
    # Example usage for testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a sample DataFrame
    data = {
        'col_a': [1, 2, None, 4, 5],
        'col_b': ['a', 'b', 'c', None, 'e'],
        'col_c': [10, 20, 30, 40, 50],
        'col_d': [None, None, None, None, None]
    }
    sample_df = pd.DataFrame(data)

    logger.info("--- Testing check_for_nulls function ---")

    # Test with columns having nulls
    columns_to_check_1 = ['col_a', 'col_b', 'col_c']
    result_1 = check_for_nulls(sample_df, columns_to_check_1)
    logger.info(f"Null counts for {columns_to_check_1}: {result_1}")
    # Expected: {'col_a': 1, 'col_b': 1}

    # Test with columns having no nulls
    columns_to_check_2 = ['col_c']
    result_2 = check_for_nulls(sample_df, columns_to_check_2)
    logger.info(f"Null counts for {columns_to_check_2}: {result_2}")
    # Expected: {}

    # Test with a non-existent column
    columns_to_check_3 = ['col_a', 'non_existent_col']
    result_3 = check_for_nulls(sample_df, columns_to_check_3)
    logger.info(f"Null counts for {columns_to_check_3}: {result_3}")
    # Expected: {'col_a': 1} and a warning for 'non_existent_col'

    # Test with all nulls
    columns_to_check_4 = ['col_d']
    result_4 = check_for_nulls(sample_df, columns_to_check_4)
    logger.info(f"Null counts for {columns_to_check_4}: {result_4}")
    # Expected: {'col_d': 5}