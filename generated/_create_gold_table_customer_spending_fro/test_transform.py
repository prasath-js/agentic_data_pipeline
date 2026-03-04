from transform import transform
import pandas as pd
import numpy as np

def test_spending_tier_high():
    """
    Test case for customers with high spending.
    """
    data = {
        'customer_id': [1],
        'total_spent': [15000.00],
        'transaction_count': [10],
        'average_transaction_value': [1500.00]
    }
    df = pd.DataFrame(data)
    result_df = transform(df)
    assert result_df['spending_tier'].iloc[0] == 'High'

def test_spending_tier_medium():
    """
    Test case for customers with medium spending.
    """
    data = {
        'customer_id': [2],
        'total_spent': [5000.00],
        'transaction_count': [5],
        'average_transaction_value': [1000.00]
    }
    df = pd.DataFrame(data)
    result_df = transform(df)
    assert result_df['spending_tier'].iloc[0] == 'Medium'

def test_spending_tier_low():
    """
    Test case for customers with low spending.
    """
    data = {
        'customer_id': [3],
        'total_spent': [500.00],
        'transaction_count': [2],
        'average_transaction_value': [250.00]
    }
    df = pd.DataFrame(data)
    result_df = transform(df)
    assert result_df['spending_tier'].iloc[0] == 'Low'

def test_spending_tier_edge_cases():
    """
    Test cases for spending tiers at boundary values and with no transactions (NaN/0).
    """
    data = {
        'customer_id': [4, 5, 6, 7, 8],
        'total_spent': [10000.01, 10000.00, 1000.01, 1000.00, 0.00],
        'transaction_count': [10, 10, 5, 5, 0],
        'average_transaction_value': [1000.001, 1000.00, 200.002, 200.00, np.nan]
    }
    df = pd.DataFrame(data)
    result_df = transform(df)
    expected_tiers = ['High', 'Medium', 'Medium', 'Low', 'Low']
    pd.testing.assert_series_equal(result_df['spending_tier'], pd.Series(expected_tiers, name='spending_tier'))

def test_empty_dataframe():
    """
    Test case for an empty input DataFrame.
    """
    df = pd.DataFrame(columns=['customer_id', 'total_spent', 'transaction_count', 'average_transaction_value'])
    result_df = transform(df)
    assert result_df.empty
    assert 'spending_tier' in result_df.columns