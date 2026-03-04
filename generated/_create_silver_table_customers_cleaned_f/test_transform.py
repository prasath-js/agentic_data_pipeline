import pandas as pd
import pytest
from __main__ import transform

def test_transform_basic_cleaning_and_email_validation():
    """
    Test basic string stripping, date parsing, and email validation
    with no duplicates.
    """
    data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['  John Doe  ', 'Jane Smith', 'Peter Jones '],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@test.com '],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln'],
        'join_date': ['2023-01-01', '2023-02-15', '2023-03-20'],
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['John Doe', 'Jane Smith', 'Peter Jones'],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@test.com'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-02-15', '2023-03-20']),
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1'],
        'email_is_valid': [True, False, True]
    }
    expected_df = pd.DataFrame(expected_data)
    # Ensure column order matches the expected output from transform
    expected_df = expected_df[df.columns.tolist() + ['email_is_valid']]

    result_df = transform(df)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_duplicate_handling_and_missing_values():
    """
    Test duplicate handling (keeping latest join_date),
    whitespace, and handling of NaN values in email/join_date.
    """
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0, 4.0],
        'name': ['  Alice  ', 'Bob', 'Alice', 'Charlie', 'Bob ', 'David'],
        'email': ['alice@example.com', 'bob@test.com', 'alice.old@example.com', None, 'bob.new@test.com', 'invalid'],
        'address': ['101 A', '202 B', '101 A', '303 C', '202 B', '404 D'],
        'join_date': ['2023-01-01', '2023-02-01', '2022-12-01', 'invalid-date', '2023-02-10', None],
        '_source_file': ['f1', 'f1', 'f1', 'f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'email': ['alice@example.com', 'bob.new@test.com', None, 'invalid'],
        'address': ['101 A', '202 B', '303 C', '404 D'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-02-10', 'NaT', 'NaT']),
        '_source_file': ['f1', 'f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1'],
        'email_is_valid': [True, True, False, False]
    }
    expected_df = pd.DataFrame(expected_data)
    # Ensure the order of columns is the same for comparison
    expected_df = expected_df[df.columns.tolist() + ['email_is_valid']]

    result_df = transform(df)

    # Sort both dataframes by customer_id before comparison to handle potential order differences
    # from drop_duplicates if the original input order was not strictly sorted.
    result_df = result_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_empty_dataframe():
    """
    Test with an empty DataFrame.
    """
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date', '_source_file', '_ingest_ts'])
    result_df = transform(df)

    expected_columns = ['customer_id', 'name', 'email', 'address', 'join_date', '_source_file', '_ingest_ts', 'email_is_valid']
    expected_df = pd.DataFrame(columns=expected_columns)
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date']) # Ensure datetime dtype for join_date
    expected_df['email_is_valid'] = expected_df['email_is_valid'].astype(bool) # Ensure bool dtype for email_is_valid

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_no_email_column():
    """
    Test behavior when the 'email' column is missing.
    email_is_valid should be False for all rows.
    """
    data = {
        'customer_id': [1.0, 2.0],
        'name': ['Test User', 'Another User'],
        'address': ['1 Test', '2 Another'],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['Test User', 'Another User'],
        'address': ['1 Test', '2 Another'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-01-02']),
        'email_is_valid': [False, False]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df = expected_df[df.columns.tolist() + ['email_is_valid']] # Maintain column order

    result_df = transform(df)
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_no_join_date_column():
    """
    Test behavior when the 'join_date' column is missing.
    Duplicates should still be dropped by customer_id.
    """
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0],
        'name': ['User A', 'User B', 'User A', 'User C'],
        'email': ['a@example.com', 'b@example.com', 'a@example.com', 'c@example.com'],
        'address': ['Addr A', 'Addr B', 'Addr A', 'Addr C']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['User A', 'User B', 'User C'],
        'email': ['a@example.com', 'b@example.com', 'c@example.com'],
        'address': ['Addr A', 'Addr B', 'Addr A', 'Addr C'],
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df = expected_df[df.columns.tolist() + ['email_is_valid']] # Maintain column order

    result_df = transform(df)
    result_df = result_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_customer_id_with_nan():
    """
    Test behavior when customer_id contains NaN values.
    NaN customer_ids should be treated as distinct by drop_duplicates.
    """
    data = {
        'customer_id': [1.0, 2.0, 1.0, float('nan'), 2.0, float('nan')],
        'name': ['A', 'B', 'A_old', 'C', 'B_new', 'D'],
        'email': ['a@a.com', 'b@b.com', 'a_old@a.com', 'c@c.com', 'b_new@b.com', 'd@d.com'],
        'join_date': ['2023-01-01', '2023-02-01', '2022-12-01', '2023-03-01', '2023-02-10', '2023-03-05']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'customer_id': [1.0, 2.0, float('nan'), float('nan')],
        'name': ['A', 'B_new', 'C', 'D'],
        'email': ['a@a.com', 'b_new@b.com', 'c@c.com', 'd@d.com'],
        'join_date': pd.to_datetime(['2023-01-01', '2023-02-10', '2023-03-01', '2023-03-05']),
        'email_is_valid': [True, True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df = expected_df[df.columns.tolist() + ['email_is_valid']]

    result_df = transform(df)

    # Sort by customer_id, NaNs will be at the end by default
    result_df = result_df.sort_values(by='customer_id', na_position='last').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id', na_position='last').reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)