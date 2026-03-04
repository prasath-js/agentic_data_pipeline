import pandas as pd
import pytest
from transform import transform

def test_strip_whitespace_and_numeric_conversion():
    data = {
        'transaction_id': [101, 102, 103, 104, 105],
        'customer_id': [1, 2, 3, 4, 5],
        'quantity': [1, 2, 3, 4, 5],
        'amount': [' $100.00 ', ' 50.50', 'invalid', ' $0.00', ' -20.00 '],
        'transaction_date': ['2023-01-01', ' 2023-01-02 ', '2023/01/03', '2023-01-04', '2023-01-05'],
        'description': [' Item A ', ' Item B', 'Item C ', 'Item D', 'Item E']
    }
    df = pd.DataFrame(data)
    cleaned_df = transform(df.copy())

    # Check whitespace stripping for string columns
    assert cleaned_df['description'].iloc[0] == 'Item A'
    assert cleaned_df['description'].iloc[1] == 'Item B'
    assert cleaned_df['description'].iloc[2] == 'Item C'

    # Check amount conversion and filtering (only > 0 amounts should remain)
    assert 'amount' in cleaned_df.columns
    assert cleaned_df['amount'].dtype == 'float64'
    assert len(cleaned_df) == 2 # 101, 102 (103 invalid, 104 zero, 105 negative dropped)
    assert cleaned_df['transaction_id'].tolist() == [101.0, 102.0]
    assert cleaned_df['amount'].tolist() == [100.0, 50.5]

    # Check transaction_date conversion
    assert cleaned_df['transaction_date'].dtype == 'datetime64[ns]'
    assert cleaned_df['transaction_date'].iloc[0] == pd.Timestamp('2023-01-01')
    assert cleaned_df['transaction_date'].iloc[1] == pd.Timestamp('2023-01-02')


def test_null_transaction_id_and_missing_columns():
    data = {
        'transaction_id': [101, None, 103, pd.NA],
        'customer_id': [1, 2, 3, 4],
        'amount': ['100', '50', '200', '300'],
        'transaction_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
    }
    df = pd.DataFrame(data)
    cleaned_df = transform(df.copy())

    # Check rows with null transaction_id are dropped
    assert len(cleaned_df) == 2
    assert cleaned_df['transaction_id'].tolist() == [101.0, 103.0]

    # Test with missing transaction_id column
    df_no_id = pd.DataFrame({
        'customer_id': [1, 2],
        'amount': ['100', '50'],
        'transaction_date': ['2023-01-01', '2023-01-02']
    })
    cleaned_df_no_id = transform(df_no_id.copy())
    # If transaction_id is missing, it's created as NA and all rows are dropped
    assert len(cleaned_df_no_id) == 0
    assert 'transaction_id' in cleaned_df_no_id.columns

    # Test with missing amount column
    df_no_amount = pd.DataFrame({
        'transaction_id': [101, 102],
        'customer_id': [1, 2],
        'transaction_date': ['2023-01-01', '2023-01-02']
    })
    cleaned_df_no_amount = transform(df_no_amount.copy())
    assert len(cleaned_df_no_amount) == 2
    assert 'amount' not in cleaned_df_no_amount.columns # amount column should not be added if not present

    # Test with missing transaction_date column
    df_no_date = pd.DataFrame({
        'transaction_id': [101, 102],
        'customer_id': [1, 2],
        'amount': ['100', '50']
    })
    cleaned_df_no_date = transform(df_no_date.copy())
    assert len(cleaned_df_no_date) == 2
    assert 'transaction_date' not in cleaned_df_no_date.columns # date column should not be added if not present
    assert cleaned_df_no_date['amount'].dtype == 'float64'