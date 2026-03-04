import pandas as pd
import pytest
from __main__ import transform

def test_transform_string_stripping():
    """
    Test that whitespace is stripped from string columns.
    """
    data = {
        'customer_id': [1, 2],
        'name': [' John Doe ', 'Jane Smith '],
        'email': [' test@example.com ', 'another@email.com'],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    assert transformed_df['name'].iloc[0] == 'John Doe'
    assert transformed_df['name'].iloc[1] == 'Jane Smith'
    assert transformed_df['email'].iloc[0] == 'test@example.com'

def test_transform_date_parsing_and_duplicates():
    """
    Test date parsing and duplicate handling (keeping latest join_date).
    """
    data = {
        'customer_id': [101, 102, 101, 103, 102],
        'name': ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'],
        'email': ['alice@example.com', 'bob@example.com', 'alice@example.com', 'charlie@example.com', 'bob_new@example.com'],
        'join_date': ['2023-01-05', '2023-01-10', '2023-01-01', '2023-01-15', '2023-01-12']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # Check date parsing
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])
    
    # Sort transformed_df by customer_id to make assertions stable
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)

    assert transformed_df['customer_id'].tolist() == [101, 102, 103]
    assert transformed_df['join_date'].iloc[0] == pd.Timestamp('2023-01-05') # Alice (latest)
    assert transformed_df['join_date'].iloc[1] == pd.Timestamp('2023-01-12') # Bob (latest)
    assert transformed_df['join_date'].iloc[2] == pd.Timestamp('2023-01-15') # Charlie

    # Check duplicate handling - should have 3 unique customers
    assert len(transformed_df) == 3
    assert sorted(transformed_df['customer_id'].tolist()) == [101, 102, 103]

    # Check that for customer_id 101, the row with '2023-01-05' was kept
    assert transformed_df[transformed_df['customer_id'] == 101]['join_date'].iloc[0] == pd.Timestamp('2023-01-05')
    # Check that for customer_id 102, the row with '2023-01-12' was kept
    assert transformed_df[transformed_df['customer_id'] == 102]['join_date'].iloc[0] == pd.Timestamp('2023-01-12')
    assert transformed_df[transformed_df['customer_id'] == 102]['email'].iloc[0] == 'bob_new@example.com'


def test_transform_email_validation():
    """
    Test the email_is_valid column creation.
    """
    data = {
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['A', 'B', 'C', 'D', 'E'],
        'email': ['valid@email.com', 'invalid-email', 'another.valid@domain.co.uk', None, '  no_at_sign.com '],
        'join_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    assert 'email_is_valid' in transformed_df.columns
    assert transformed_df['email_is_valid'].iloc[0] == True
    assert transformed_df['email_is_valid'].iloc[1] == False
    assert transformed_df['email_is_valid'].iloc[2] == True
    assert transformed_df['email_is_valid'].iloc[3] == False # None should be False
    assert transformed_df['email_is_valid'].iloc[4] == False # Stripped 'no_at_sign.com' should be False

def test_transform_empty_dataframe():
    """
    Test transformation on an empty DataFrame.
    """
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date'])
    transformed_df = transform(df)

    assert transformed_df.empty
    assert 'email_is_valid' in transformed_df.columns
    # Check if 'join_date' column exists and is datetime, or if it doesn't exist
    if 'join_date' in transformed_df.columns:
        assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])
    else:
        # If join_date was not in the original empty df, it won't be added as datetime
        # The original logic only converts if it exists.
        pass


def test_transform_missing_columns():
    """
    Test transformation when some expected columns are missing.
    """
    data = {
        'customer_id': [1, 2],
        'name': ['Test User', 'Another User']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # join_date should not be present or should not cause error
    assert 'join_date' not in transformed_df.columns or not pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])
    # email_is_valid should be added and be False
    assert 'email_is_valid' in transformed_df.columns
    assert all(transformed_df['email_is_valid'] == False)
    # Duplicates should still be handled by customer_id if join_date is missing
    data_dup = {
        'customer_id': [1, 1, 2],
        'name': ['Test User', 'Test User Dup', 'Another User']
    }
    df_dup = pd.DataFrame(data_dup)
    transformed_df_dup = transform(df_dup)
    assert len(transformed_df_dup) == 2
    assert sorted(transformed_df_dup['customer_id'].tolist()) == [1, 2]
    # Ensure the first occurrence is kept for customer_id 1
    assert transformed_df_dup[transformed_df_dup['customer_id'] == 1]['name'].iloc[0] == 'Test User'