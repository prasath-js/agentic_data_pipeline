import pandas as pd
import pytest
from datetime import datetime

# Assuming the clean_transactions function is in a file named 'transform.py'
# For this setup, we'll assume it's directly available or imported from the same file.
# from transform import clean_transactions 

# If running this as a standalone test file, you might need to adjust the import
# or copy the function here. For the purpose of this generation, we assume
# the function is available in the context where tests are run.

# Helper function to make the clean_transactions function available for testing
# in this generated output. In a real scenario, it would be imported.
def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw transactions DataFrame according to the specified rules.

    Args:
        df (pd.DataFrame): The input DataFrame with raw transaction data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Ensure transaction_id column exists and drop rows where it is null
    if 'transaction_id' in df.columns:
        df = df.dropna(subset=['transaction_id'])
    else:
        pass # No action needed if column doesn't exist for dropping nulls

    # 3. Coerce amount column to numeric (errors='coerce')
    # 4. Drop rows where amount is null or amount <= 0
    if 'amount' in df.columns:
        # Remove non-numeric characters like '$' before coercing
        df['amount'] = df['amount'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['amount'])
        df = df[df['amount'] > 0]
    else:
        pass

    # 5. Parse transaction_date to datetime if the column exists
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    
    return df


def test_transactions_cleaning_basic_flow():
    """
    Tests the basic cleaning flow: stripping, numeric conversion, null/zero amount filtering,
    and date parsing.
    """
    data = {
        'transaction_id': [101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
        'customer_id': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        'quantity': [1, 2, 3, 4, 5, 6],
        'amount': [' $100.00 ', '50.50', 'invalid', '$0.00', '-10.00', ' 75.25 '],
        'transaction_date': ['2023-01-01 ', '2023-01-02', 'invalid-date', '2023-01-04', '2023-01-05', '2023-01-06'],
        '_source_file': ['f1', 'f1', 'f1', 'f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1']
    }
    df_raw = pd.DataFrame(data)

    df_cleaned = clean_transactions(df_raw.copy())

    # Expected results after cleaning
    # Rows with 'invalid' amount, '$0.00', '-10.00' should be dropped.
    # Row with 'invalid-date' should have NaT for transaction_date.
    # Whitespace should be stripped.
    expected_data = {
        'transaction_id': [101.0, 102.0, 106.0],
        'customer_id': [1.0, 2.0, 6.0],
        'quantity': [1, 2, 6],
        'amount': [100.00, 50.50, 75.25],
        'transaction_date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-06')],
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1']
    }
    df_expected = pd.DataFrame(expected_data)
    df_expected['transaction_date'] = df_expected['transaction_date'].dt.normalize() # Normalize to remove time component for comparison

    # Check if the cleaned DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(
        df_cleaned.reset_index(drop=True),
        df_expected.reset_index(drop=True),
        check_dtype=True,
        check_exact=False, # Allow for floating point differences
        atol=1e-9 # Absolute tolerance for floating point comparison
    )
    
    # Verify column dtypes
    assert df_cleaned['amount'].dtype == float
    assert pd.api.types.is_datetime64_any_dtype(df_cleaned['transaction_date'])
    assert df_cleaned['_source_file'].dtype == object # Should still be object/string

def test_transactions_cleaning_edge_cases():
    """
    Tests edge cases: null transaction_id, missing columns, and all invalid amounts.
    """
    data = {
        'transaction_id': [101.0, None, 103.0, 104.0],
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'quantity': [1, 2, 3, 4],
        'amount': ['100.00', 'invalid', '0.00', '-5.00'],
        'transaction_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
        '_source_file': ['f1', 'f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1']
    }
    df_raw = pd.DataFrame(data)

    df_cleaned = clean_transactions(df_raw.copy())

    # Expected:
    # - Row with transaction_id=None should be dropped.
    # - Rows with 'invalid', '0.00', '-5.00' amounts should be dropped.
    # Only the first row should remain.
    expected_data = {
        'transaction_id': [101.0],
        'customer_id': [1.0],
        'quantity': [1],
        'amount': [100.00],
        'transaction_date': [pd.Timestamp('2023-01-01')],
        '_source_file': ['f1'],
        '_ingest_ts': ['ts1']
    }
    df_expected = pd.DataFrame(expected_data)
    df_expected['transaction_date'] = df_expected['transaction_date'].dt.normalize()

    pd.testing.assert_frame_equal(
        df_cleaned.reset_index(drop=True),
        df_expected.reset_index(drop=True),
        check_dtype=True,
        check_exact=False,
        atol=1e-9
    )

    # Test with missing 'transaction_id' and 'transaction_date' columns
    data_missing_cols = {
        'customer_id': [1.0, 2.0],
        'quantity': [1, 2],
        'amount': ['100.00', '50.00'],
        '_source_file': ['f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1']
    }
    df_raw_missing = pd.DataFrame(data_missing_cols)
    df_cleaned_missing = clean_transactions(df_raw_missing.copy())

    # Should still clean amount and strip strings, but no date parsing or transaction_id null drop
    expected_missing_cols = {
        'customer_id': [1.0, 2.0],
        'quantity': [1, 2],
        'amount': [100.00, 50.00],
        '_source_file': ['f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1']
    }
    df_expected_missing = pd.DataFrame(expected_missing_cols)

    pd.testing.assert_frame_equal(
        df_cleaned_missing.reset_index(drop=True),
        df_expected_missing.reset_index(drop=True),
        check_dtype=True,
        check_exact=False,
        atol=1e-9
    )

    # Test with an empty DataFrame
    df_empty = pd.DataFrame(columns=['transaction_id', 'customer_id', 'quantity', 'amount', 'transaction_date', '_source_file', '_ingest_ts'])
    df_cleaned_empty = clean_transactions(df_empty.copy())
    pd.testing.assert_frame_equal(df_cleaned_empty, df_empty, check_dtype=True)