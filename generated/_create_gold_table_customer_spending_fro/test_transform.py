import pandas as pd
import numpy as np
import pytest
from __main__ import transform # Assuming transform is in the same file for testing

def test_high_spending_customer():
    data = {
        'customer_id': [1],
        'name': ['John Doe'],
        'email': ['john.doe@example.com'],
        'address': ['123 Main St'],
        'join_date': [pd.Timestamp('2023-01-01')],
        'total_spent': [15000.0],
        'transaction_count': [10]
    }
    df = pd.DataFrame(data)
    result = transform(df)

    assert result.loc[0, 'average_transaction_value'] == 1500.0
    assert result.loc[0, 'spending_tier'] == 'High'

def test_medium_spending_customer():
    data = {
        'customer_id': [2],
        'name': ['Jane Smith'],
        'email': ['jane.smith@example.com'],
        'address': ['456 Oak Ave'],
        'join_date': [pd.Timestamp('2023-02-01')],
        'total_spent': [5000.0],
        'transaction_count': [5]
    }
    df = pd.DataFrame(data)
    result = transform(df)

    assert result.loc[0, 'average_transaction_value'] == 1000.0
    assert result.loc[0, 'spending_tier'] == 'Medium'

def test_low_spending_customer():
    data = {
        'customer_id': [3],
        'name': ['Peter Jones'],
        'email': ['peter.jones@example.com'],
        'address': ['789 Pine Ln'],
        'join_date': [pd.Timestamp('2023-03-01')],
        'total_spent': [500.0],
        'transaction_count': [2]
    }
    df = pd.DataFrame(data)
    result = transform(df)

    assert result.loc[0, 'average_transaction_value'] == 250.0
    assert result.loc[0, 'spending_tier'] == 'Low'

def test_customer_with_no_transactions():
    data = {
        'customer_id': [4],
        'name': ['Alice Brown'],
        'email': ['alice.brown@example.com'],
        'address': ['101 Cedar Rd'],
        'join_date': [pd.Timestamp('2023-04-01')],
        'total_spent': [0.0],
        'transaction_count': [0]
    }
    df = pd.DataFrame(data)
    result = transform(df)

    # average_transaction_value should be 0 when transaction_count is 0
    assert result.loc[0, 'average_transaction_value'] == 0.0
    assert result.loc[0, 'spending_tier'] == 'Low'

def test_mixed_customers():
    data = {
        'customer_id': [1, 2, 3, 4],
        'name': ['John Doe', 'Jane Smith', 'Peter Jones', 'Alice Brown'],
        'email': ['john.doe@example.com', 'jane.smith@example.com', 'peter.jones@example.com', 'alice.brown@example.com'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln', '101 Cedar Rd'],
        'join_date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-02-01'), pd.Timestamp('2023-03-01'), pd.Timestamp('2023-04-01')],
        'total_spent': [15000.0, 5000.0, 500.0, 0.0],
        'transaction_count': [10, 5, 2, 0]
    }
    df = pd.DataFrame(data)
    result = transform(df)

    assert result.loc[0, 'average_transaction_value'] == 1500.0
    assert result.loc[0, 'spending_tier'] == 'High'

    assert result.loc[1, 'average_transaction_value'] == 1000.0
    assert result.loc[1, 'spending_tier'] == 'Medium'

    assert result.loc[2, 'average_transaction_value'] == 250.0
    assert result.loc[2, 'spending_tier'] == 'Low'

    assert result.loc[3, 'average_transaction_value'] == 0.0
    assert result.loc[3, 'spending_tier'] == 'Low'