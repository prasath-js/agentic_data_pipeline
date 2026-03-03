import pandas as pd
import pytest
# The transform function is defined above, so it's available in this scope for testing.

def test_strip_whitespace():
    """Test that whitespace is stripped from string columns."""
    data = {
        'customer_id': [1.0, 2.0],
        'name': [' John Doe ', 'Jane Smith '],
        'email': [' test@example.com', 'another@example.com '],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy()) # Use .copy() to avoid modifying original df

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['John Doe', 'Jane Smith'],
        'email': ['test@example.com', 'another@example.com'],
        'join_date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')],
        'email_is_valid': [True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    # Ensure join_date dtype matches
    expected_df['join_date'] = expected_df['join_date'].astype(transformed_df['join_date'].dtype)

    pd.testing.assert_frame_equal(transformed_df.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=True)

def test_parse_join_date_and_email_validation():
    """Test join_date parsing and email validation, including invalid dates and emails."""
    data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'email': ['alice@example.com', 'bob.com', None, 'charlie@test.com '],
        'join_date': ['2023-01-01', 'invalid-date', '2023-03-15', None]
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0, 4.0],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'email': ['alice@example.com', 'bob.com', None, 'charlie@test.com'],
        'join_date': [pd.Timestamp('2023-01-01'), pd.NaT, pd.Timestamp('2023-03-15'), pd.NaT],
        'email_is_valid': [True, False, False, True]
    }
    expected_df = pd.DataFrame(expected_data)
    # Ensure join_date dtype matches
    expected_df['join_date'] = expected_df['join_date'].astype(transformed_df['join_date'].dtype)

    pd.testing.assert_frame_equal(transformed_df.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=True)

def test_drop_duplicates_latest_join_date():
    """Test dropping duplicate customer_ids, keeping the one with the latest join_date."""
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0],
        'name': ['Alice', 'Bob', 'Alice_old', 'Charlie', 'Bob_new'],
        'email': ['alice@example.com', 'bob@example.com', 'alice_old@example.com', 'charlie@example.com', 'bob_new@example.com'],
        'address': ['Addr1', 'Addr2', 'Addr1_old', 'Addr3', 'Addr2_new'],
        'join_date': ['2023-01-01', '2023-02-01', '2023-01-15', '2023-03-01', '2023-02-10']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['Alice_old', 'Bob_new', 'Charlie'], # Alice_old (2023-01-15) > Alice (2023-01-01)
                                                      # Bob_new (2023-02-10) > Bob (2023-02-01)
        'email': ['alice_old@example.com', 'bob_new@example.com', 'charlie@example.com'],
        'address': ['Addr1_old', 'Addr2_new', 'Addr3'],
        'join_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10'), pd.Timestamp('2023-03-01')],
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    # Ensure join_date dtype matches
    expected_df['join_date'] = expected_df['join_date'].astype(transformed_df['join_date'].dtype)

    # Sort both for consistent comparison as drop_duplicates might not preserve original order
    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_empty_dataframe():
    """Test transformation with an empty DataFrame."""
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date'])
    transformed_df = transform(df.copy())

    expected_df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date', 'email_is_valid'])
    # Set dtypes explicitly to match what transform would produce
    expected_df['customer_id'] = expected_df['customer_id'].astype('float64') # Based on sample
    expected_df['name'] = expected_df['name'].astype('object')
    expected_df['email'] = expected_df['email'].astype('object')
    expected_df['address'] = expected_df['address'].astype('object')
    expected_df['join_date'] = expected_df['join_date'].astype('datetime64[ns]') # pd.to_datetime default for empty
    expected_df['email_is_valid'] = expected_df['email_is_valid'].astype('bool')

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)

def test_dataframe_missing_columns():
    """Test transformation when some expected columns are missing."""
    data = {
        'customer_id': [1.0, 2.0],
        'name': ['Alice', 'Bob'],
        'address': ['Addr1', 'Addr2']
        # Missing 'email' and 'join_date'
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0],
        'name': ['Alice', 'Bob'],
        'address': ['Addr1', 'Addr2'],
        'email_is_valid': [False, False] # Should be False if email column is missing
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(transformed_df.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=True)

def test_duplicates_with_nan_join_date():
    """Test dropping duplicates when join_date contains NaN values."""
    data = {
        'customer_id': [1.0, 2.0, 1.0, 3.0, 2.0],
        'name': ['Alice', 'Bob', 'Alice_old', 'Charlie', 'Bob_new'],
        'email': ['alice@example.com', 'bob@example.com', 'alice_old@example.com', 'charlie@example.com', 'bob_new@example.com'],
        'join_date': ['2023-01-01', None, '2023-01-15', '2023-03-01', '2023-02-10']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df.copy())

    expected_data = {
        'customer_id': [1.0, 2.0, 3.0],
        'name': ['Alice_old', 'Bob_new', 'Charlie'], # Alice_old (2023-01-15) > Alice (2023-01-01)
                                                      # Bob_new (2023-02-10) > Bob (None)
        'email': ['alice_old@example.com', 'bob_new@example.com', 'charlie@example.com'],
        'join_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10'), pd.Timestamp('2023-03-01')],
        'email_is_valid': [True, True, True]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['join_date'] = expected_df['join_date'].astype(transformed_df['join_date'].dtype)

    transformed_df = transformed_df.sort_values(by='customer_id').reset_index(drop=True)
    expected_df = expected_df.sort_values(by='customer_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=True)