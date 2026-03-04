import pandas as pd
import pytest
from transform import transform # Import the transform function from the main module

def test_transform_basic_cleaning():
    data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': [' John Doe ', 'Jane Smith', '  Peter Jones'],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@example.com '],
        'address': [' 123 Main St', '456 Oak Ave ', '789 Pine Ln'],
        'join_date': ['2023-01-01', '2023-02-15', '2023-03-10'],
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['John Doe', 'Jane Smith', 'Peter Jones'],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@example.com'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-02-15', '2023-03-10']),
        'email_is_valid': [True, False, True],
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df)

def test_transform_duplicate_handling():
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0],
        'name': ['John A', 'Jane B', 'John C', 'Peter D', 'Jane E'],
        'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com', 'e@e.com'],
        'address': ['1', '2', '3', '4', '5'],
        'join_date': ['2023-01-01', '2023-02-01', '2023-01-05', '2023-03-01', '2023-02-10'],
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    # Expected:
    # customer_id 1.0: keep 'John C' (2023-01-05) over 'John A' (2023-01-01)
    # customer_id 2.0: keep 'Jane E' (2023-02-10) over 'Jane B' (2023-02-01)
    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['John C', 'Jane E', 'Peter D'],
        'email': ['c@c.com', 'e@e.com', 'd@d.com'],
        'address': ['3', '5', '4'],
        'join_date': pd.to_datetime(['2023-01-05', '2023-02-10', '2023-03-01']),
        'email_is_valid': [True, True, True],
    }
    expected_df = pd.DataFrame(expected_data)

    # Sort both for consistent comparison, as the transform function sorts by customer_id
    # before dropping duplicates, resulting in a sorted output by customer_id.
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df)

def test_transform_missing_columns():
    data = {
        'customer_id': [1.0, 2.0],
        'name': [' John ', ' Jane '],
        'address': ['1', '2'],
        # 'join_date' is missing
        # 'email' is missing
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['John', 'Jane'],
        'address': ['1', '2'],
        'email_is_valid': [False, False], # Should be False if email column is missing
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df)