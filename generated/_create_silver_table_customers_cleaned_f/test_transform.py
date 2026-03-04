import pandas as pd
import pytest

# Assuming this file is named 'transform.py' or similar,
# and pytest will import the transform function from it.
from transform import transform

def test_strip_whitespace_and_email_validation():
    data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': [' John Doe ', 'Jane Smith', '  Bob  '],
        'email': ['john@example.com ', 'invalid-email', 'bob@test.com'],
        'address': ['123 Main St', '456 Oak Ave ', '789 Pine Ln'],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['John Doe', 'Jane Smith', 'Bob'],
        'email': ['john@example.com', 'invalid-email', 'bob@test.com'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'email_is_valid': [True, False, True]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_drop_duplicates_and_latest_join_date():
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0, 1.0],
        'name': ['John', 'Jane', 'John_old', 'Alice', 'Jane_old', 'John_latest'],
        'email': ['john@a.com', 'jane@b.com', 'john_old@a.com', 'alice@c.com', 'jane_old@b.com', 'john_latest@a.com'],
        'address': ['Addr1', 'Addr2', 'Addr1_old', 'Addr3', 'Addr2_old', 'Addr1_latest'],
        'join_date': ['2023-01-01', '2023-01-05', '2023-01-10', '2023-01-03', '2023-01-02', '2023-01-15']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['John_latest', 'Jane', 'Alice'],
        'email': ['john_latest@a.com', 'jane@b.com', 'alice@c.com'],
        'address': ['Addr1_latest', 'Addr2', 'Addr3'],
        'join_date': pd.to_datetime(['2023-01-15', '2023-01-05', '2023-01-03']),
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    
    # Sort both for consistent comparison as drop_duplicates preserves order based on sort
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_missing_columns_robustness():
    data = {
        'customer_id': [1.0, 2.0, 1.0],
        'name': ['Test1', 'Test2', 'Test1_new'],
        'address': ['AddrA', 'AddrB', 'AddrA_new']
        # Missing 'email' and 'join_date'
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['Test1', 'Test2'], # Keeps first for customer_id 1.0 as no join_date to sort by
        'address': ['AddrA', 'AddrB'],
        'email_is_valid': [False, False] # Should default to False if email column is missing
    }
    expected_df = pd.DataFrame(expected_data)
    
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)