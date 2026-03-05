import pandas as pd
import pytest
from transform import transform # This import assumes 'transform' is a module or accessible in the path

def test_strip_whitespace_and_email_validation():
    """
    Test stripping whitespace from string columns and email validation.
    """
    data = {
        'customer_id': [1, 2, 3],
        'name': ['  John Doe  ', 'Jane Smith', 'Bob'],
        'email': ['john@example.com ', 'invalid-email', ' bob@test.com'],
        'address': [' 123 Main St ', '456 Oak Ave', '789 Pine Ln'],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1, 2, 3],
        'name': ['John Doe', 'Jane Smith', 'Bob'],
        'email': ['john@example.com', 'invalid-email', 'bob@test.com'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'email_is_valid': [True, False, True]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_duplicate_handling_and_date_parsing():
    """
    Test dropping duplicate customer_ids, keeping the latest join_date,
    and date parsing with mixed date formats.
    """
    data = {
        'customer_id': [101, 102, 101, 103, 102],
        'name': ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'],
        'email': ['alice@a.com', 'bob@b.com', 'alice@a.com', 'charlie@c.com', 'bob@b.com'],
        'address': ['Addr1', 'Addr2', 'Addr1_old', 'Addr3', 'Addr2_old'],
        'join_date': ['2023-01-05', '2023-01-10', '2023-01-01', '2023-01-15', '2023-01-08']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [101, 102, 103],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@a.com', 'bob@b.com', 'charlie@c.com'],
        'address': ['Addr1', 'Addr2', 'Addr3'],
        'join_date': pd.to_datetime(['2023-01-05', '2023-01-10', '2023-01-15']),
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_missing_columns():
    """
    Test behavior when 'join_date' or 'email' columns are missing.
    """
    data = {
        'customer_id': [1, 2],
        'name': ['Test User', 'Another User'],
        'address': ['1 Test St', '2 Another Ave']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1, 2],
        'name': ['Test User', 'Another User'],
        'address': ['1 Test St', '2 Another Ave'],
        'email_is_valid': [False, False]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_empty_dataframe():
    """
    Test with an empty DataFrame.
    """
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date'])
    transformed_df = transform(df)

    expected_df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date', 'email_is_valid'])
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date']) # Ensure datetime dtype for join_date
    expected_df['email_is_valid'] = expected_df['email_is_valid'].astype(bool) # Ensure bool dtype for email_is_valid

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_join_date_with_nulls_and_invalid_formats():
    """
    Test join_date parsing with nulls and invalid formats.
    """
    data = {
        'customer_id': [1, 2, 3, 4],
        'name': ['A', 'B', 'C', 'D'],
        'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com'],
        'join_date': ['2023-01-01', None, 'invalid-date', '2023/02/01']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    expected_data = {
        'customer_id': [1, 2, 3, 4],
        'name': ['A', 'B', 'C', 'D'],
        'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com'],
        'join_date': pd.to_datetime(['2023-01-01', pd.NaT, pd.NaT, '2023-02-01']),
        'email_is_valid': [True, True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)