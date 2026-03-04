import pandas as pd
import pytest
from transform import transform

def test_strip_whitespace_and_null_transaction_id():
    data = {
        'transaction_id': [1.0, 2.0, None, 4.0],
        'customer_id': [101, 102, 103, 104],
        'quantity': [1, 2, 3, 4],
        'amount': [' 10.00 ', '20.00', '30.00', '40.00'],
        'transaction_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
        'string_col_with_space': ['  hello  ', 'world', '  test  ', 'data']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    # Expect row with null transaction_id to be dropped
    assert len(transformed_df) == 3
    assert 3.0 not in transformed_df['transaction_id'].values

    # Expect whitespace to be stripped
    assert transformed_df['string_col_with_space'].tolist() == ['hello', 'world', 'data']
    assert transformed_df['amount'].tolist() == [10.00, 20.00, 40.00] # After amount processing

def test_amount_and_date_parsing():
    data = {
        'transaction_id': [1.0, 2.0, 3.0, 4.0, 5.0],
        'customer_id': [101, 102, 103, 104, 105],
        'quantity': [1, 2, 3, 4, 5],
        'amount': [' $10.50 ', '20.00', 'invalid', '-5.00', None],
        'transaction_date': ['2023-01-01', '2023-01-02', 'invalid-date', '2023-01-04', None]
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    # Expect rows with invalid/null/negative amount to be dropped
    # Original rows: 1.0, 2.0, 3.0, 4.0, 5.0
    # Valid amounts: 1.0 ($10.50), 2.0 (20.00)
    # Invalid amounts: 3.0 (invalid), 4.0 (-5.00), 5.0 (None)
    assert len(transformed_df) == 2
    assert transformed_df['transaction_id'].tolist() == [1.0, 2.0]

    # Expect amount to be numeric
    assert transformed_df['amount'].dtype == 'float64'
    assert transformed_df['amount'].tolist() == [10.50, 20.00]

    # Expect transaction_date to be datetime, invalid dates coerced to NaT
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['transaction_date'])
    assert transformed_df['transaction_date'].iloc[0] == pd.Timestamp('2023-01-01')
    assert transformed_df['transaction_date'].iloc[1] == pd.Timestamp('2023-01-02')

def test_no_transaction_id_column():
    data = {
        'customer_id': [101, 102],
        'amount': ['10.00', '20.00'],
        'transaction_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    # If 'transaction_id' column doesn't exist, no rows should be dropped based on it.
    # The current implementation handles this by doing nothing if the column is missing.
    assert len(transformed_df) == 2
    assert 'transaction_id' not in transformed_df.columns # Column should not be added

def test_amount_with_dollar_sign():
    data = {
        'transaction_id': [1.0, 2.0],
        'amount': ['$100.00', '$25.50'],
        'transaction_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())
    assert transformed_df['amount'].tolist() == [100.00, 25.50]
    assert transformed_df['amount'].dtype == 'float64'

def test_empty_dataframe():
    df = pd.DataFrame(columns=['transaction_id', 'customer_id', 'quantity', 'amount', 'transaction_date'])
    transformed_df = transform(df.copy())
    assert transformed_df.empty
    assert list(transformed_df.columns) == ['transaction_id', 'customer_id', 'quantity', 'amount', 'transaction_date']