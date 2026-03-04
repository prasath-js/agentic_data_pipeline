import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

from transform import transform

def test_transform_spending_tiers():
    """
    Test cases for different spending tiers: High, Medium, Low, and boundary conditions.
    """
    data = {
        'customer_id': [1, 2, 3, 4, 5, 6, 7],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace'],
        'email': ['a@example.com', 'b@example.com', 'c@example.com', 'd@example.com', 'e@example.com', 'f@example.com', 'g@example.com'],
        'total_spent': [15000.0, 5000.0, 500.0, 10000.0, 1000.0, 0.0, np.nan],
        'transaction_count': [100, 50, 10, 80, 20, 0, 0],
        'average_transaction_value': [150.0, 100.0, 50.0, 125.0, 50.0, np.nan, np.nan]
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1, 2, 3, 4, 5, 6, 7],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace'],
        'email': ['a@example.com', 'b@example.com', 'c@example.com', 'd@example.com', 'e@example.com', 'f@example.com', 'g@example.com'],
        'total_spent': [15000.0, 5000.0, 500.0, 10000.0, 1000.0, 0.0, np.nan],
        'transaction_count': [100, 50, 10, 80, 20, 0, 0],
        'average_transaction_value': [150.0, 100.0, 50.0, 125.0, 50.0, np.nan, np.nan],
        'spending_tier': ['High', 'Medium', 'Low', 'Medium', 'Low', 'Low', 'Low']
    }
    df_expected = pd.DataFrame(expected_data)

    df_actual = transform(df_input.copy())

    df_actual = df_actual.sort_values('customer_id').reset_index(drop=True)
    df_expected = df_expected.sort_values('customer_id').reset_index(drop=True)

    assert_frame_equal(df_actual, df_expected, check_dtype=False)

def test_transform_empty_dataframe():
    """
    Test with an empty DataFrame.
    """
    df_input = pd.DataFrame(columns=[
        'customer_id', 'name', 'email', 'total_spent', 'transaction_count', 'average_transaction_value'
    ])
    df_expected = df_input.copy()
    df_expected['spending_tier'] = pd.Series(dtype=str)

    df_actual = transform(df_input.copy())

    assert_frame_equal(df_actual, df_expected, check_dtype=False)

def test_transform_all_high_spending():
    """
    Test case where all customers are high spending.
    """
    data = {
        'customer_id': [101, 102],
        'name': ['Xavier', 'Yara'],
        'email': ['x@example.com', 'y@example.com'],
        'total_spent': [20000.0, 10001.0],
        'transaction_count': [200, 101],
        'average_transaction_value': [100.0, 100.0099]
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'customer_id': [101, 102],
        'name': ['Xavier', 'Yara'],
        'email': ['x@example.com', 'y@example.com'],
        'total_spent': [20000.0, 10001.0],
        'transaction_count': [200, 101],
        'average_transaction_value': [100.0, 100.0099],
        'spending_tier': ['High', 'High']
    }
    df_expected = pd.DataFrame(expected_data)

    df_actual = transform(df_input.copy())

    assert_frame_equal(df_actual, df_expected, check_dtype=False)