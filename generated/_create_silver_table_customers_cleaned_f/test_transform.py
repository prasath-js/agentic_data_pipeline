import pandas as pd
import pytest
from datetime import datetime, date
from transform import transform_customers

def test_strip_whitespace():
    """
    Test that whitespace is stripped from string columns.
    """
    data = {
        'customer_id': [1, 2],
        'name': [' John Doe ', 'Jane Smith '],
        'email': [' test@example.com ', 'another@example.com'],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform_customers(df)

    assert transformed_df['name'].iloc[0] == 'John Doe'
    assert transformed_df['name'].iloc[1] == 'Jane Smith'
    assert transformed_df['email'].iloc[0] == 'test@example.com'
    assert transformed_df['email'].iloc[1] == 'another@example.com'

def test_parse_join_date():
    """
    Test that join_date is correctly parsed to datetime objects.
    Also test handling of invalid dates.
    """
    data = {
        'customer_id': [1, 2, 3],
        'name': ['John', 'Jane', 'Peter'],
        'join_date': ['2023-01-01', '2023-02-15', 'invalid-date']
    }
    df = pd.DataFrame(data)
    transformed_df = transform_customers(df)

    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])
    assert transformed_df['join_date'].iloc[0] == datetime(2023, 1, 1)
    assert transformed_df['join_date'].iloc[1] == datetime(2023, 2, 15)
    assert pd.isna(transformed_df['join_date'].iloc[2])

def test_drop_duplicates_latest_join_date():
    """
    Test that duplicate customer_ids are dropped, keeping the row with the latest join_date.
    """
    data = {
        'customer_id': [1, 2, 1, 3, 2],
        'name': ['John A', 'Jane A', 'John B', 'Alice', 'Jane B'],
        'email': ['john@a.com', 'jane@a.com', 'john@b.com', 'alice@c.com', 'jane@b.com'],
        'join_date': ['2023-01-01', '2023-01-05', '2023-01-02', '2023-01-10', '2023-01-04']
    }
    df = pd.DataFrame(data)
    transformed_df = transform_customers(df)

    expected_data = {
        'customer_id': [1, 2, 3],
        'name': ['John B', 'Jane A', 'Alice'],
        'email': ['john@b.com', 'jane@a.com', 'alice@c.com'],
        'join_date': [datetime(2023, 1, 2), datetime(2023, 1, 5), datetime(2023, 1, 10)],
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    # Sort both for comparison as order might change due to drop_duplicates
    pd.testing.assert_frame_equal(
        transformed_df.sort_values(by='customer_id').reset_index(drop=True),
        expected_df.sort_values(by='customer_id').reset_index(drop=True),
        check_dtype=False # join_date might have different timezone info
    )

def test_email_is_valid_column():
    """
    Test the creation of the email_is_valid boolean column.
    """
    data = {
        'customer_id': [1, 2, 3, 4],
        'name': ['John', 'Jane', 'Peter', 'Mary'],
        'email': ['valid@example.com', 'invalid-email', 'another@domain.co.uk', None],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
    }
    df = pd.DataFrame(data)
    transformed_df = transform_customers(df)

    assert 'email_is_valid' in transformed_df.columns
    assert transformed_df['email_is_valid'].iloc[0] is True
    assert transformed_df['email_is_valid'].iloc[1] is False
    assert transformed_df['email_is_valid'].iloc[2] is True
    assert transformed_df['email_is_valid'].iloc[3] is False # None should result in False

def test_empty_dataframe():
    """
    Test handling of an empty DataFrame.
    """
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'join_date'])
    transformed_df = transform_customers(df)

    assert transformed_df.empty
    assert 'email_is_valid' in transformed_df.columns
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])

def test_missing_columns():
    """
    Test handling when some expected columns are missing.
    """
    data = {
        'customer_id': [1, 2],
        'name': ['John', 'Jane']
        # email and join_date are missing
    }
    df = pd.DataFrame(data)
    transformed_df = transform_customers(df)

    assert 'customer_id' in transformed_df.columns
    assert 'name' in transformed_df.columns
    assert 'email_is_valid' in transformed_df.columns
    assert transformed_df['email_is_valid'].iloc[0] is False # Should default to False if email column is missing
    assert 'join_date' not in transformed_df.columns # Should not be added if not present
    assert len(transformed_df) == 2 # No duplicates to drop without join_date or email
    assert transformed_df['name'].iloc[0] == 'John' # Should still strip whitespace if present