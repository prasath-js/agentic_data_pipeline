import hashlib
import logging
from typing import Any, Dict, List, Union

import pandas as pd

logger = logging.getLogger(__name__)

def mask_pii(value: str) -> str:
    """
    Masks a string value using SHA-256 hashing.

    Args:
        value (str): The string value to mask.

    Returns:
        str: The SHA-256 hash of the input value.
    """
    if not isinstance(value, str):
        logger.warning("Attempted to mask a non-string value. Returning original value.")
        return value
    return hashlib.sha256(value.encode()).hexdigest()

def standardize_date_column(df: pd.DataFrame, column_name: str, date_format: str = '%Y-%m-%d') -> pd.DataFrame:
    """
    Converts a specified column in a DataFrame to datetime objects with a standard format.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to standardize.
        date_format (str): The desired output format for the date. Defaults to '%Y-%m-%d'.

    Returns:
        pd.DataFrame: The DataFrame with the specified column converted to datetime and formatted.
    """
    if column_name not in df.columns:
        logger.warning(f"Date column '{column_name}' not found in DataFrame. Skipping standardization.")
        return df

    try:
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df[column_name] = df[column_name].dt.strftime(date_format)
        logger.info(f"Successfully standardized date column '{column_name}'.")
    except (ValueError, TypeError) as e:
        logger.error(f"Error standardizing date column '{column_name}': {e}")
    return df

def validate_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> bool:
    """
    Validates if the DataFrame's columns and their data types match an expected schema.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        expected_schema (Dict[str, str]): A dictionary where keys are column names
                                          and values are expected pandas data types (e.g., 'object', 'int64', 'float64', 'datetime64[ns]').

    Returns:
        bool: True if the DataFrame schema matches the expected schema, False otherwise.
    """
    df_columns = set(df.columns)
    expected_columns = set(expected_schema.keys())

    # Check for missing columns
    missing_columns = expected_columns - df_columns
    if missing_columns:
        logger.error(f"Schema validation failed: Missing expected columns: {missing_columns}")
        return False

    # Check for unexpected columns (optional, depending on strictness)
    # If you want to allow extra columns, remove this block
    unexpected_columns = df_columns - expected_columns
    if unexpected_columns:
        logger.warning(f"Schema validation warning: Unexpected columns found: {unexpected_columns}")
        # Depending on strictness, you might return False here, but for now, it's a warning.

    # Check column data types
    for col, expected_dtype in expected_schema.items():
        if col in df.columns: # Check again in case it was an unexpected column earlier
            actual_dtype = str(df[col].dtype)
            if actual_dtype != expected_dtype:
                logger.error(f"Schema validation failed for column '{col}': "
                             f"Expected dtype '{expected_dtype}', got '{actual_dtype}'.")
                return False
    
    logger.info("Schema validation successful.")
    return True

def cast_dataframe_columns(df: pd.DataFrame, column_types: Dict[str, Any]) -> pd.DataFrame:
    """
    Casts specified columns in a DataFrame to the given data types.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_types (Dict[str, Any]): A dictionary mapping column names to desired data types.
                                        e.g., {'quantity': 'int64', 'unit_price': 'float64'}

    Returns:
        pd.DataFrame: The DataFrame with columns cast to the specified types.
    """
    for col, dtype in column_types.items():
        if col in df.columns:
            try:
                # Special handling for datetime objects if 'datetime64[ns]' is specified
                if dtype == 'datetime64[ns]':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                else:
                    df[col] = df[col].astype(dtype)
                logger.debug(f"Column '{col}' cast to type '{dtype}'.")
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to cast column '{col}' to type '{dtype}': {e}. Column type remains unchanged.")
        else:
            logger.warning(f"Column '{col}' not found in DataFrame for type casting.")
    return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Running helper utilities tests...")

    # Test PII masking
    test_value = "john.doe@example.com"
    masked_value = mask_pii(test_value)
    logger.info(f"Original: {test_value}, Masked: {masked_value}")
    assert masked_value == hashlib.sha256(test_value.encode()).hexdigest()
    assert mask_pii(123) == 123 # Should return original for non-strings
    logger.info("PII masking test passed.")

    # Test date standardization
    data_dates = {'order_id': [1, 2, 3], 'order_date': ['2023-01-15', '1/16/2023', '20230117'], 'ship_date': ['01-01-2023', 'invalid', '2023-01-03']}
    df_dates = pd.DataFrame(data_dates)
    logger.info(f"Original DataFrame:\n{df_dates}")
    df_dates_standardized = standardize_date_column(df_dates.copy(), 'order_date')
    logger.info(f"Standardized 'order_date':\n{df_dates_standardized}")
    assert df_dates_standardized['order_date'].iloc[0] == '2023-01-15'
    assert df_dates_standardized['order_date'].iloc[1] == '2023-01-16'
    assert df_dates_standardized['order_date'].iloc[2] == '2023-01-17'
    df_dates_standardized_invalid = standardize_date_column(df_dates.copy(), 'invalid_column')
    assert df_dates_standardized_invalid.equals(df_dates)
    logger.info("Date standardization test passed.")

    # Test schema validation
    data_schema = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C'], 'col3': [1.1, 2.2, 3.3]}
    df_schema = pd.DataFrame(data_schema)
    logger.info(f"DataFrame for schema validation:\n{df_schema}")

    expected_schema_pass = {'col1': 'int64', 'col2': 'object', 'col3': 'float64'}
    assert validate_schema(df_schema, expected_schema_pass)

    expected_schema_fail_dtype = {'col1': 'object', 'col2': 'object', 'col3': 'float64'}
    assert not validate_schema(df_schema, expected_schema_fail_dtype)

    expected_schema_fail_missing = {'col1': 'int64', 'col2': 'object', 'col4': 'str'}
    assert not validate_schema(df_schema, expected_schema_fail_missing)

    expected_schema_pass_with_extra = {'col1': 'int64', 'col2': 'object'} # df_schema has col3 which is extra
    assert validate_schema(df_schema, expected_schema_pass_with_extra) # Should still pass with warning

    logger.info("Schema validation test passed.")

    # Test column type casting
    data_cast = {
        'int_col': ['1', '2', '3'],
        'float_col': ['1.1', '2.2', 'invalid_float'],
        'date_col': ['2023-01-01', '2023-01-02', 'invalid_date'],
        'str_col': [1, 2, 3]
    }
    df_cast = pd.DataFrame(data_cast)
    logger.info(f"Original DataFrame for casting:\n{df_cast.dtypes}")

    column_types_to_cast = {
        'int_col': 'int64',
        'float_col': 'float64',
        'date_col': 'datetime64[ns]',
        'new_col': 'str' # Column not in DataFrame
    }

    df_cast_result = cast_dataframe_columns(df_cast.copy(), column_types_to_cast)
    logger.info(f"DataFrame after casting:\n{df_cast_result.dtypes}")

    assert str(df_cast_result['int_col'].dtype) == 'int64'
    assert str(df_cast_result['float_col'].dtype) == 'float64'
    assert str(df_cast_result['date_col'].dtype) == 'datetime64[ns]'
    assert pd.isna(df_cast_result['float_col'].iloc[2]) # 'invalid_float' should be NaN
    assert pd.isna(df_cast_result['date_col'].iloc[2]) # 'invalid_date' should be NaT

    logger.info("Column type casting test passed.")

    logger.info("All helper utilities tests completed.")
