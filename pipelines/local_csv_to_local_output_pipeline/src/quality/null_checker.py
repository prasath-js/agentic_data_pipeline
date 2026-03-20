import pandas as pd
import logging
from typing import List, Dict, Union

logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: Union[List[str], None] = None) -> Dict[str, int]:
    """
    Checks for null values in specified columns of a DataFrame.

    If `columns` is None, it checks all columns. Logs the findings.

    Args:
        df (pd.DataFrame): The DataFrame to check for nulls.
        columns (Union[List[str], None]): A list of column names to check. If None, all columns are checked.

    Returns:
        Dict[str, int]: A dictionary where keys are column names and values are the
                        count of nulls in that column. Only columns with nulls are included.
    """
    if df.empty:
        logger.warning("DataFrame is empty, no nulls to check.")
        return {}

    if columns is None:
        columns_to_check = df.columns
    else:
        columns_to_check = [col for col in columns if col in df.columns]
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Columns not found in DataFrame for null check: {missing_columns}")

    null_counts = df[columns_to_check].isnull().sum()
    null_report = null_counts[null_counts > 0].to_dict()

    if null_report:
        for col, count in null_report.items():
            logger.warning(f"Column '{col}' has {count} null values.")
    else:
        logger.info("No null values found in the specified columns.")

    return null_report

if __name__ == '__main__':
    # Example usage for standalone testing
    import os
    os.environ["LOG_LEVEL"] = "INFO"
    from src.config.logging_config import configure_logging
    configure_logging()

    data_with_nulls = {
        'col_A': [1, 2, None, 4, 5],
        'col_B': ['a', 'b', 'c', None, 'e'],
        'col_C': [10.1, 11.2, 12.3, 13.4, 14.5],
        'col_D': [None, None, None, None, None]
    }
    df_test_nulls = pd.DataFrame(data_with_nulls)

    data_no_nulls = {
        'col_X': [1, 2, 3],
        'col_Y': ['x', 'y', 'z']
    }
    df_test_no_nulls = pd.DataFrame(data_no_nulls)

    print("--- Testing DataFrame with Nulls (specific columns) ---")
    nulls_in_ab = check_for_nulls(df_test_nulls, ['col_A', 'col_B'])
    print(f"Nulls in A, B: {nulls_in_ab}")
    assert nulls_in_ab == {'col_A': 1, 'col_B': 1}

    print("\n--- Testing DataFrame with Nulls (all columns) ---")
    nulls_all = check_for_nulls(df_test_nulls)
    print(f"Nulls in all columns: {nulls_all}")
    assert nulls_all == {'col_A': 1, 'col_B': 1, 'col_D': 5}

    print("\n--- Testing DataFrame with No Nulls ---")
    nulls_none = check_for_nulls(df_test_no_nulls)
    print(f"Nulls in no-nulls DataFrame: {nulls_none}")
    assert nulls_none == {}

    print("\n--- Testing with empty DataFrame ---")
    empty_df = pd.DataFrame()
    nulls_empty = check_for_nulls(empty_df)
    print(f"Nulls in empty DataFrame: {nulls_empty}")
    assert nulls_empty == {}

    print("\n--- Testing with non-existent columns ---")
    nulls_non_existent = check_for_nulls(df_test_nulls, ['col_A', 'col_Z'])
    print(f"Nulls with non-existent column: {nulls_non_existent}")
    assert nulls_non_existent == {'col_A': 1}

    print("\nAll null_checker tests passed.")