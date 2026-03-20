# src/quality/null_checker.py
import pandas as pd
import logging
from typing import List, Literal, Optional

logger = logging.getLogger(__name__)

def check_and_handle_nulls(
    df: pd.DataFrame,
    columns: List[str],
    strategy: Literal["warn", "drop", "fill_mean", "fill_median", "fill_mode", "fill_value"] = "warn",
    fill_value: Optional[Any] = None
) -> pd.DataFrame:
    """
    Checks for null values in specified columns of a DataFrame and applies a handling strategy.

    Args:
        df (pd.DataFrame): The input DataFrame to check.
        columns (List[str]): A list of column names to check for nulls.
        strategy (Literal): The strategy to apply when nulls are found:
                            - "warn": Log a warning.
                            - "drop": Drop rows containing nulls in the specified columns.
                            - "fill_mean": Fill nulls with the mean of the column (for numeric types).
                            - "fill_median": Fill nulls with the median of the column (for numeric types).
                            - "fill_mode": Fill nulls with the mode of the column.
                            - "fill_value": Fill nulls with a specified `fill_value`.
        fill_value (Optional[Any]): The value to use if strategy is "fill_value".

    Returns:
        pd.DataFrame: The DataFrame after applying the null handling strategy.
    """
    if df.empty:
        logger.warning("DataFrame is empty, skipping null check.")
        return df

    df_copy = df.copy()
    null_found = False
    for col in columns:
        if col not in df_copy.columns:
            logger.warning(f"Column '{col}' not found in DataFrame for null checking.")
            continue

        null_count = df_copy[col].isnull().sum()
        if null_count > 0:
            null_found = True
            logger.warning(f"Column '{col}' has {null_count} null values.")

            if strategy == "drop":
                original_rows = len(df_copy)
                df_copy.dropna(subset=[col], inplace=True)
                logger.info(f"Dropped {original_rows - len(df_copy)} rows due to nulls in '{col}'.")
            elif strategy == "fill_mean":
                if pd.api.types.is_numeric_dtype(df_copy[col]):
                    mean_val = df_copy[col].mean()
                    df_copy[col].fillna(mean_val, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with mean value: {mean_val:.2f}.")
                else:
                    logger.warning(f"Cannot fill non-numeric column '{col}' with mean. Strategy ignored.")
            elif strategy == "fill_median":
                if pd.api.types.is_numeric_dtype(df_copy[col]):
                    median_val = df_copy[col].median()
                    df_copy[col].fillna(median_val, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with median value: {median_val:.2f}.")
                else:
                    logger.warning(f"Cannot fill non-numeric column '{col}' with median. Strategy ignored.")
            elif strategy == "fill_mode":
                mode_val = df_copy[col].mode()[0] if not df_copy[col].mode().empty else None
                if mode_val is not None:
                    df_copy[col].fillna(mode_val, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with mode value: {mode_val}.")
                else:
                    logger.warning(f"Cannot determine mode for column '{col}'. Strategy ignored.")
            elif strategy == "fill_value":
                if fill_value is not None:
                    df_copy[col].fillna(fill_value, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with specified value: {fill_value}.")
                else:
                    logger.warning(f"Fill value not provided for strategy 'fill_value' in column '{col}'. Strategy ignored.")
            elif strategy == "warn":
                pass # Warning already logged above
            else:
                logger.warning(f"Unknown null handling strategy '{strategy}' for column '{col}'. No action taken.")

    if not null_found and columns:
        logger.info(f"No null values found in specified columns: {', '.join(columns)}.")
    elif not columns:
        logger.info("No columns specified for null checking.")

    return df_copy