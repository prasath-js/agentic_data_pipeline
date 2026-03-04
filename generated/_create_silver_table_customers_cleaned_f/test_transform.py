import pandas as pd
import pytest
from __main__ import transform

def test_strip_whitespace_and_email_validation():
    """
    Test stripping whitespace from string columns and email validation.
    """
    data = {
        'customer_id': [1, 2, 3],
        'name': [' John Doe ', 'Jane Smith', '  Peter Pan  '],
        'email': ['john.doe@example.com', 'invalid-email', 'peter.pan@domain.co.uk '],
        'address': ['123 Main St ', '456 Oak Ave', '789 Pine Ln'],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # Check whitespace stripping
    assert transformed_df.loc[0, 'name'] == 'John Doe'
    assert transformed_df.loc[2, 'name'] == 'Peter Pan'
    assert transformed_df.loc[0, 'address'] == '123 Main St'
    assert transformed_df.loc[2, 'email'] == 'peter.pan@domain.co.uk' # Trailing space removed

    # Check email validation
    assert transformed_df.loc[0, 'email_is_valid'] is True
    assert transformed_df.loc[1, 'email_is_valid'] is False
    assert transformed_df.loc[2, 'email_is_valid'] is True

def test_date_parsing_and_duplicate_handling():
    """
    Test join_date parsing and dropping duplicate customer_ids,
    keeping the one with the latest join_date.
    """
    data = {
        'customer_id': [101, 102, 101, 103, 102],
        'name': ['Alice', 'Bob', 'Alice Duplicate', 'Charlie', 'Bob Duplicate'],
        'email': ['alice@example.com', 'bob@example.com', 'alice_dup@example.com', 'charlie@example.com', 'bob_dup@example.com'],
        'join_date': ['2023-03-10', '2023-03-15', '2023-03-12', '2023-03-05', '2023-03-14'],
        'address': ['Addr1', 'Addr2', 'Addr1_dup', 'Addr3', 'Addr2_dup']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # Check number of unique customers
    assert len(transformed_df) == 3
    assert set(transformed_df['customer_id']) == {101, 102, 103}

    # Check if the correct duplicate was kept for customer_id 101 (latest join_date)
    alice_row = transformed_df[transformed_df['customer_id'] == 101].iloc[0]
    assert alice_row['name'] == 'Alice Duplicate'
    assert alice_row['join_date'] == pd.Timestamp('2023-03-12')

    # Check if the correct duplicate was kept for customer_id 102 (latest join_date)
    bob_row = transformed_df[transformed_df['customer_id'] == 102].iloc[0]
    assert bob_row['name'] == 'Bob'
    assert bob_row['join_date'] == pd.Timestamp('2023-03-15')

    # Check date column type
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])

def test_missing_columns_robustness():
    """
    Test the transform function's robustness when 'join_date' or 'email' columns are missing.
    """
    data_no_join_date = {
        'customer_id': [1, 2, 1],
        'name': ['Test1', 'Test2', 'Test1_dup'],
        'email': ['test1@example.com', 'test2@example.com', 'test1_dup@example.com']
    }
    df_no_join_date = pd.DataFrame(data_no_join_date)
    transformed_df_no_join_date = transform(df_no_join_date)

    # Should still drop duplicates based on customer_id, keeping first encountered
    assert len(transformed_df_no_join_date) == 2
    assert 'join_date' not in transformed_df_no_join_date.columns # Column should not be added if not present
    assert transformed_df_no_join_date.loc[0, 'name'] == 'Test1' # First 'Test1' should be kept

    data_no_email = {
        'customer_id': [1, 2],
        'name': ['Test1', 'Test2'],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df_no_email = pd.DataFrame(data_no_email)
    transformed_df_no_email = transform(df_no_email)

    # email_is_valid should be added and be False
    assert 'email_is_valid' in transformed_df_no_email.columns
    assert all(transformed_df_no_email['email_is_valid'] == False)
    assert pd.api.types.is_datetime64_any_dtype(transformed_df_no_email['join_date'])

    data_empty = pd.DataFrame(columns=['customer_id', 'name', 'email', 'join_date'])
    transformed_df_empty = transform(data_empty)
    assert transformed_df_empty.empty
    assert 'email_is_valid' in transformed_df_empty.columns
    assert pd.api.types.is_datetime64_any_dtype(transformed_df_empty['join_date'])