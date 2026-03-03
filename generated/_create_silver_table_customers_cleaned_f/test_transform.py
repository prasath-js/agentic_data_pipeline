import pandas as pd
import pytest
from __main__ import transform

def test_transform_basic_cleaning():
    data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': [' John Doe ', 'Jane Smith', '  Peter Pan  ', 'Alice'],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@example.com ', None],
        'address': ['123 Main St', '456 Oak Ave ', '789 Pine Ln', '101 Elm St'],
        'join_date': ['2023-01-01', '2023-02-15', '2023-03-20', 'invalid-date'],
        '_source_file': ['f1', 'f1', 'f2', 'f2'],
        '_ingest_ts': ['ts1', 'ts1', 'ts2', 'ts2']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['John Doe', 'Jane Smith', 'Peter Pan', 'Alice'],
        'email': ['john.doe@example.com', 'invalid-email', 'peter@example.com', None],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Ln', '101 Elm St'],
        'join_date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-02-15'), pd.Timestamp('2023-03-20'), pd.NaT],
        '_source_file': ['f1', 'f1', 'f2', 'f2'],
        '_ingest_ts': ['ts1', 'ts1', 'ts2', 'ts2'],
        'email_is_valid': [True, False, True, False]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date'])

    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_transform_duplicate_handling():
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0],
        'name': ['John', 'Jane', 'Johnny', 'Peter', 'Janet'],
        'email': ['john@example.com', 'jane@example.com', 'johnny@example.com', 'peter@example.com', 'janet@example.com'],
        'address': ['1A', '2B', '1B', '3C', '2C'],
        'join_date': ['2023-01-01', '2023-02-01', '2023-01-05', '2023-03-01', '2023-01-20'],
        '_source_file': ['f1', 'f1', 'f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1', 'ts1']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['Johnny', 'Jane', 'Peter'],
        'email': ['johnny@example.com', 'jane@example.com', 'peter@example.com'],
        'address': ['1B', '2B', '3C'],
        'join_date': [pd.Timestamp('2023-01-05'), pd.Timestamp('2023-02-01'), pd.Timestamp('2023-03-01')],
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1'],
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date'])

    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_transform_missing_columns():
    data = {
        'customer_id': [1.0, 2.0, 1.0],
        'name': ['John', 'Jane', 'Johnny'],
        'address': ['1A', '2B', '1B'],
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['John', 'Jane'],
        'address': ['1A', '2B'],
        '_source_file': ['f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1'],
        'email_is_valid': [False, False]
    }
    expected_df = pd.DataFrame(expected_data)

    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_transform_empty_dataframe():
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date', '_source_file', '_ingest_ts'])
    transformed_df = transform(df.copy())

    expected_df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date', '_source_file', '_ingest_ts', 'email_is_valid'])
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date'])
    expected_df['email_is_valid'] = expected_df['email_is_valid'].astype(bool)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_transform_email_non_string_types():
    data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['A', 'B', 'C', 'D'],
        'email': ['valid@email.com', 123, 45.67, None],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['A', 'B', 'C', 'D'],
        'email': ['valid@email.com', 123, 45.67, None],
        'join_date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-03'), pd.Timestamp('2023-01-04')],
        'email_is_valid': [True, False, False, False]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['join_date'] = pd.to_datetime(expected_df['join_date'])

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)