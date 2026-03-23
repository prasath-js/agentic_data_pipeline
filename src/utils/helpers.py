import hashlib
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


def mask_pii(value: Optional[Union[str, int]]) -> Optional[str]:
    """
    Masks a PII value using SHA-256 hashing.

    Args:
        value (Optional[Union[str, int]]): The PII value to mask.

    Returns:
        Optional[str]: The SHA-256 hash of the value if not None, otherwise None.
    """
    if value is None:
        return None
    try:
        return hashlib.sha256(str(value).encode('utf-8')).hexdigest()
    except (UnicodeEncodeError, TypeError) as e:
        logger.error(f"Error masking PII value: {e}")
        return None


def standardize_date_column(df: pd.DataFrame, column_name: str, date_format: str = '%Y-%m-%d') -> pd.DataFrame:
    """
    Standardizes a date column in a DataFrame to a specified format.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the date column to standardize.
        date_format (str): The desired output date format (e.g., '%Y-%m-%d').

    Returns:
        pd.DataFrame: The DataFrame with the date column standardized.
    """
    if column_name not in df.columns:
        logger.warning(f"Date column '{column_name}' not found in DataFrame. Skipping standardization.")
        return df

    try:
        # Attempt to convert to datetime, coercing errors to NaT
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        # Format back to string, handling NaT values
        df[column_name] = df[column_name].dt.strftime(date_format)
        logger.info(f"Successfully standardized date column '{column_name}' to format '{date_format}'.")
    except (ValueError, AttributeError, TypeError) as e:
        logger.error(f"Error standardizing date column '{column_name}': {e}")
    return df


def validate_schema(df: pd.DataFrame, expected_schema: Dict[str, Any]) -> bool:
    """
    Validates if a DataFrame's schema matches the expected schema.
    Checks for column presence and data types.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        expected_schema (Dict[str, Any]): A dictionary where keys are column names
                                          and values are expected data types (e.g., str, int, float, datetime).

    Returns:
        bool: True if the schema matches, False otherwise.
    """
    is_valid = True
    df_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    # Check for missing columns
    missing_columns = expected_columns - df_columns
    if missing_columns:
        logger.error(f"Schema validation failed: Missing expected columns: {missing_columns}")
        is_valid = False

    # Check for unexpected columns (optional, depends on strictness)
    unexpected_columns = df_columns - expected_columns
    if unexpected_columns:
        logger.warning(f"Schema validation warning: Unexpected columns found: {unexpected_columns}")

    # Check data types for common columns
    for col, expected_type in expected_schema.items():
        if col in df.columns:
            # For robustness, we check the inferred pandas dtype against a mapping
            # This can be tricky with 'object' types that might contain mixed data
            # A more robust check might involve sampling and checking individual values
            # Here, we do a basic check.
            actual_dtype = df[col].dtype
            if pd.api.types.is_numeric_dtype(actual_dtype) and (expected_type is int or expected_type is float):
                continue
            elif pd.api.types.is_string_dtype(actual_dtype) and expected_type is str:
                continue
            elif pd.api.types.is_datetime64_any_dtype(actual_dtype) and expected_type is pd.Timestamp:
                continue
            elif pd.api.types.is_bool_dtype(actual_dtype) and expected_type is bool:
                continue
            elif (actual_dtype == object) and (expected_type is str or expected_type is Any):
                # 'object' can be anything, often strings. This is a lenient check.
                continue
            else:
                logger.error(f"Schema validation failed for column '{col}': Expected type {expected_type.__name__}, got {actual_dtype}")
                is_valid = False
        elif col not in missing_columns:
            # This case should ideally not happen if missing_columns is handled above,
            # but as a safeguard.
            logger.error(f"Schema validation failed: Column '{col}' not found for type check.")
            is_valid = False

    if is_valid:
        logger.info("Schema validation successful.")
    else:
        logger.error("Schema validation failed.")

    return is_valid


def main() -> None:
    """
    Main function to demonstrate helper utilities.
    """
    logging.basicConfig(level=logging.INFO)
    logger.info("Demonstrating helper utilities.")

    # PII Masking demonstration
    original_name = "John Doe"
    masked_name = mask_pii(original_name)
    logger.info(f"Original name: {original_name}, Masked name: {masked_name}")

    original_email = "john.doe@example.com"
    masked_email = mask_pii(original_email)
    logger.info(f"Original email: {original_email}, Masked email: {masked_email}")

    # Date standardization demonstration
    data = {
        'order_id': [1, 2, 3],
        'order_date_raw': ['2023-01-01', '02/Jan/2023', '2023-1-3'],
        'sale_amount': [100.50, 200.00, 150.75]
    }
    df_dates = pd.DataFrame(data)
    logger.info("\nOriginal DataFrame with raw dates:")
    logger.info(df_dates)

    df_standardized = standardize_date_column(df_dates.copy(), 'order_date_raw', '%Y-%m-%d')
    logger.info("\nDataFrame with standardized dates:")
    logger.info(df_standardized)

    df_standardized_us = standardize_date_column(df_dates.copy(), 'order_date_raw', '%m/%d/%Y')
    logger.info("\nDataFrame with standardized dates (MM/DD/YYYY):")
    logger.info(df_standardized_us)

    # Schema validation demonstration
    sales_data = {
        'order_id': [101, 102, 103],
        'customer_id': [1, 2, 1],
        'customer_name': ['Alice', 'Bob', 'Alice'],
        'product_id': ['P001', 'P002', 'P001'],
        'quantity': [2, 1, 3],
        'total_amount': [20.0, 15.0, 30.0],
        'order_date': ['2023-01-15', '2023-01-16', '2023-01-17']
    }
    df_sales = pd.DataFrame(sales_data)
    df_sales['order_date'] = pd.to_datetime(df_sales['order_date']) # Convert to datetime for schema check

    expected_sales_schema = {
        'order_id': int,
        'customer_id': int,
        'customer_name': str,
        'product_id': str,
        'quantity': int,
        'total_amount': float,
        'order_date': pd.Timestamp
    }

    logger.info("\nValidating sales DataFrame against expected schema:")
    is_valid_sales_schema = validate_schema(df_sales, expected_sales_schema)
    logger.info(f"Sales DataFrame schema is valid: {is_valid_sales_schema}")

    # Demonstrate invalid schema
    invalid_sales_data = {
        'order_id': ['101', '102', '103'], # Should be int, not str
        'customer_id': [1, 2, 1],
        'customer_name': ['Alice', 'Bob', 'Alice'],
        'quantity': [2, 1, 3],
        'total_amount': [20.0, 15.0, 30.0],
        'order_date': ['2023-01-15', '2023-01-16', '2023-01-17']
    }
    df_invalid_sales = pd.DataFrame(invalid_sales_data)
    df_invalid_sales['order_date'] = pd.to_datetime(df_invalid_sales['order_date'])

    logger.info("\nValidating invalid sales DataFrame against expected schema (missing 'product_id', wrong 'order_id' type):")
    is_valid_invalid_sales_schema = validate_schema(df_invalid_sales, expected_sales_schema)
    logger.info(f"Invalid Sales DataFrame schema is valid: {is_valid_invalid_sales_schema}")


if __name__ == "__main__":
    main()
